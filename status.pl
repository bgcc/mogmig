#!/usr/bin/env perl
# prints progress information for mogmig on stdout.

use strict;
use warnings;
use DBI;
use IO::Handle;

sub setup_sqlite {
	my %db = ();
	my $dbh = DBI->connect('dbi:SQLite:/var/tmp/migration.db');
	$db{dbh} = $dbh;

	$db{status} = $dbh->prepare('SELECT COUNT(*) FROM files;');
	$db{errors} = $dbh->prepare('SELECT COUNT(*) FROM error_log;');
	$db{errortailf} = $dbh->prepare('SELECT * FROM error_log LIMIT 10 OFFSET ?;');

	\%db;
}

my $db = setup_sqlite;
my $prev = 0; # previous files
my $eoffs = 0;
my $errors_since_start = 0;
my $files_at_start = 0;
my $files_since_start = 0; # files copied since start
my $start = time;
my $previteration = time;

while (1) {
	$db->{status}->execute;
	my @f = $db->{status}->fetchrow_array;
	
	$db->{errors}->execute;
	my @e = $db->{errors}->fetchrow_array;

	$eoffs = $e[0] unless $eoffs; # initialize
	$files_at_start = $f[0] unless $files_at_start;
	$files_since_start = $f[0] - $files_at_start;
	$prev = $f[0] unless $prev;

	$db->{errortailf}->execute($eoffs);
	while (my @row = $db->{errortailf}->fetchrow_array()) {
		$eoffs++;
		$errors_since_start++;
		my $r0 = $row[0] || '""';
		my $r6 = $row[6] ? '"'.$row[6].'"' : '""';
		print "$r0 $row[1] $row[2] $row[3] $row[4] $row[5] $r6\n";
	}

	my $now = time;
	print $now-$start,"s elapsed, $f[0]/$files_since_start files, $e[0]/$errors_since_start errors, ",
		sprintf('%.2f',($f[0]-$prev)/($now-$previteration+0.0000001)),
		" files per second",(" "x10)."\r";
	STDOUT->flush;

	$prev = $f[0];
	$previteration = $now;
	sleep 1;
}
