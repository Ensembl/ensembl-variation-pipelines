#!/usr/bin/env perl
use strict;
use warnings;
use DBI qw(:sql_types);

# The workflow stages each writer's database under a distinct shard directory.
my ($output) = @ARGV;
die "Usage: merge_prediction_databases.pl OUTPUT\n" unless defined $output;
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
$db->commit;
$db->disconnect;
