#!/usr/bin/env perl

# Copyright (c) 2016 BGCC GmbH
# Licensed for use and redistribution under the same terms as Perl itself.

# FIXME: better handling of child process crashes
# FIXME: status reporting - I currently look at the SQLite database for status info (see status.pl)

use strict;
use warnings;

use IO::Pipe;
use Carp;
use DBI;
use MogileFS::Utils;
use LWP;
use File::Path qw/make_path/;
use File::Spec;
use Try::Tiny;

my $util = MogileFS::Utils->new;
my $usage = "--trackers=host --domain=foo --key_prefixes='bar/,baz/,bleh/' --num_workers=42";
my $c = $util->getopts($usage, qw/key_prefixes=s@ num_workers=i/);

my @prefixes = split(/,/,join(',', @{$c->{key_prefixes} or ['']}));
my $num_workers = $c->{num_workers} || 4;

our $mogc = $util->client;
our %workers = ();

sub select_sleep {
	# sleep with subsecond precision
	select undef, undef, undef, shift;
}

sub warning {
	print STDERR "Warning ($$): ", shift, "\n";
}

sub setup_sqlite {
	# this is only its own function because I don't want the heredocs to
	# look ugly all over the place.

	my %db = ();
	my $dbh = DBI->connect('dbi:SQLite:/var/tmp/migration.db');
	$db{dbh} = $dbh;

	$dbh->sqlite_busy_timeout(60*1000); #1min, but if it starts failing here, you probably have too many workers

	# error_log status:
	# - 1 == file exists only in the database but not on the file system
	# - 2 == file exists only in the file system but not in the database - overwrite it and log a warning
	# - 3 == unable to fetch file from one of its reported locations
	# - 4 == unable to fetch file from any of its reported locations
	# - 5 == unable to get paths for this file from mogilefs

	$dbh->do(<<SQL);

CREATE TABLE IF NOT EXISTS error_log(
	id INTEGER,
	pid INTEGER NOT NULL,
	time INTEGER NOT NULL,
	path TEXT NOT NULL,
	local_path TEXT NOT NULL,
	status INTEGER NOT NULL,
	message TEXT
);
SQL
	$dbh->do(<<SQL);
CREATE TABLE IF NOT EXISTS files(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	path TEXT UNIQUE NOT NULL,
	local_path TEXT UNIQUE NOT NULL
);
SQL

	$dbh->{HandleError} = sub { confess(shift) };

	$db{insert} = $dbh->prepare(<<SQL);
INSERT INTO files(path,local_path) VALUES (?,?);
SQL
	$db{check} = $dbh->prepare(<<SQL);
SELECT id FROM files WHERE path = ?
SQL

	$db{log} = $dbh->prepare(<<SQL);
INSERT INTO error_log VALUES (?,?,strftime('%s'),?,?,?,?);
SQL
	\%db;
}

# Child process: Flag to determine whether to quit the workers after downloading the current file
# Master process: Flag to determine whether to kill all children with SIGINT and terminate itself
our $terminate = 0;
$SIG{INT} = sub { $terminate = 1; };

sub worker_main {
	my $db = setup_sqlite;
	my ($master_pid, $pipe) = @_;
	select_sleep rand(2); # wait up to two seconds to add some jitter

	while (<$pipe>) {
		exit 1 if $terminate; # SIGINT received?

		last unless chomp; # strip newline, quit if line isn't terminated (broken pipe)

		my $key = $_; # key = "path" of the file that mogilefs exposes to the outside

		### UNCOMMENT THIS NEXT LINE IF YOU WANT TO RENAME THE
		### FILES/MOVE THEM TO SUBDIRECTORIES AS YOU GO
		#s,/(.*(..))\.,/$2/$1.,; # perform filename substitution on the destination file

		my $local_path = $_; # name of the destination file on the local filesystem

		$db->{check}->execute($key);

		@_ = $db->{check}->fetchrow_array();
		my $id = shift;

		my $isfile = -f $local_path;

		if ($id && $isfile) {
			# not an error - file exists and the database knows about it
			next; 
		} elsif ($id) {
			# file exists only in the database but not on the file system
			$db->{log}->execute($id, $$, $key, $local_path, 1, undef);
			next;
		} elsif ($isfile) {
			# file exists only in the file system but not in the
			# database - overwrite it and log a warning
			$db->{log}->execute($id, $$, $key, $local_path, 2, undef);
		}

		## if it falls through here, fetch the file because it does not yet exist or might be truncated

		my (undef, $directory, undef) = File::Spec->splitpath($local_path);
		make_path $directory unless -d $directory;

		my @paths;
		my $retry = 0;
		my $tries = 0;
		do {
			try {
				@paths = $mogc->get_paths($key, { noverify => 1 });
				$retry = 0;
			} catch {
				if (/^MogileFS::Backend: timed out/) {
					$retry = 1;
					$tries++;
				} else {
					die @_;
				}
			};
		} while ($retry);

		warning "Had to poke mogilefs $tries times for key $key before I got a response :(" if $tries;

		if ($mogc->errcode) {
			$db->{log}->execute($id, $$, $key, $local_path, 5, $mogc->errstr);
			next;
		}

		my @resses = (); # storage for the mogilefs operation results
		for my $path (@paths) {
			next unless $path; # overparanoid?
			my $ua = LWP::UserAgent->new;
			$ua->timeout(10);

			my $res = $ua->get($path,
				#':content_cb' => sub { print $file $_[0]; },
				':content_file' => $local_path,
				':read_size_hint' => 32768
			);

			if ($res->is_success) {
				$db->{insert}->execute($key, $local_path);
				last;
			}

			$db->{log}->execute($id, $$, $key, $local_path, 3, $path);
			# print all the errors to be the most helpful
			push @resses, $res;
		}

		foreach my $res (@resses) {
			$db->{log}->execute($id, $$, $key, $local_path, 4, $res->status_line);
		}
	}
}

sub fork_worker {
	my $pipe = IO::Pipe->new();
	my $master_pid = $$;
	my $pid = fork;
	if ($pid == 0) {
		$pipe->reader();
		close $_ foreach values %workers; # close other pipes
		worker_main $master_pid, $pipe;
		exit;
	} else {
		$pipe->writer();
		$workers{$pid} = $pipe;
	}
}

fork_worker for (1..$num_workers);
my @worker_queue = keys %workers;

foreach my $prefix (@prefixes) {

	my $each = sub {
		my $key = shift;

		if ($terminate) {
			print "Terminating child processes: ";
			kill 'INT', @worker_queue;
			print wait, " " and STDOUT->flush foreach @worker_queue;
			print "\n";
		 	exit 1;
		}

		# round-robin over all worker processes
		my $worker = shift @worker_queue;
		my $pipe = $workers{$worker};
		print $pipe "$key\n";
		$pipe->flush;
		push @worker_queue, $worker;
	};

	my $last = "";
	my $max = 1000;
	my $count = $max;
	while ($count == $max) {
		my $done = 0;
		my $res;
		my $tries = 0;
		my $prevretry = '';

		do {
			try {
				# This is from the MogileFS::Client/foreach_key
				# source, because it kept aborting and had to
				# be restarted for the entire prefix.
				$res = $mogc->{backend}->do_request
					("list_keys", {
					 domain => $mogc->{domain},
					 prefix => $prefix,
					 after => $last,
					 limit => $max,
				}) and $done = 1;
			} catch {
				$tries = ($last eq $prevretry) ? $tries+1 : 0;
				warning "Retrying mogilefs backend request from $last (#$tries)";
				select_sleep .2; # don't hammer too fast
				$prevretry = $last;
			};
		} while(!$done);
			
		$count = $res->{key_count}+0;
		for (my $i = 1; $i <= $count; $i++) {
			$each->($res->{"key_$i"});
		}
		$last = $res->{"key_$count"};
	}
}

close $workers{$_} foreach @worker_queue;
print "Waiting for child processes to finish: ";
print wait, " " and STDOUT->flush foreach @worker_queue;

if ($mogc->errcode) {
	die "Error listing files: ". $mogc->errstr;
}
