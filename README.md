This tool was written to make migration of a relatively large mogilefs into a
SAN possible.

It starts multiple worker processes and passes MogileFS key names to them via a
pipe. These workers check whether the file is already present and if not,
download it, optionally rename it (you have to edit the code and modify a
regular expression) and store it in an sqlite3 database which is used to keep
track of which files were already completely downloaded.

All files which are not present in the sqlite database are considered
incomplete and will be overwritten.

