#!/usr/bin/env perl
use strict;
use warnings;
use DBI qw(:sql_types);

# The workflow stages each writer's database under a distinct shard directory.
# Any remaining arguments are key=value rows for the output's meta table, which
# records the versions of the tools whose output is in the matrices.
my ($output, @meta) = @ARGV;
die "Usage: merge_prediction_databases.pl OUTPUT [KEY=VALUE ...]\n" unless defined $output;
unlink $output or die "Cannot remove $output: $!" if -e $output;
my $options = { RaiseError => 1, PrintError => 0, AutoCommit => 1 };
my $db = DBI->connect("dbi:SQLite:dbname=$output", "", "", $options);
$db->do('CREATE TABLE predictions (md5 TEXT NOT NULL, analysis INTEGER NOT NULL, matrix BLOB NOT NULL, PRIMARY KEY (md5, analysis))');
my $insert = $db->prepare('INSERT INTO predictions (md5, analysis, matrix) VALUES (?, ?, ?)');
$db->begin_work;
for my $path (sort glob('shards/part*/*.db')) {
  my $source = DBI->connect("dbi:SQLite:dbname=$path", "", "", $options);
  my $rows = $source->prepare('SELECT md5, analysis, matrix FROM predictions');
  $rows->execute;
  while (my $row = $rows->fetchrow_arrayref) {
    $insert->bind_param(1, $row->[0]);
    $insert->bind_param(2, $row->[1]);
    $insert->bind_param(3, $row->[2], SQL_BLOB);
    $insert->execute;
  }
  $rows->finish;
  $source->disconnect;
}
$db->do('CREATE INDEX md5_idx ON predictions(md5)');
if (@meta) {
  $db->do('CREATE TABLE meta (meta_key TEXT NOT NULL PRIMARY KEY, meta_value TEXT NOT NULL)');
  my $insert_meta = $db->prepare('INSERT OR REPLACE INTO meta (meta_key, meta_value) VALUES (?, ?)');
  for my $entry (@meta) {
    my ($key, $value) = split /=/, $entry, 2;
    die "Malformed meta entry '$entry': expected KEY=VALUE\n"
      unless defined $key && length $key && defined $value && length $value;
    $insert_meta->execute($key, $value);
  }
}
$db->commit;
$db->disconnect;
