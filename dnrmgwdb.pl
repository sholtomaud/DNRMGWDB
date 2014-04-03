#!/usr/bin/perl -w
use FindBin qw($Bin);
use lib "$Bin/lib"; 
use DNRMGWDB;
use Data::Dumper;
use constant DB_DIR => $Bin.'/gwdb/'; 

mkdir DB_DIR if (! -d DB_DIR );

my $import = DNRMGWDB->new(
  db_dir        => DB_DIR,
  db_name       => 'gwdb.db',
  import_dir    => $Bin.'/dnrmdata',
  import_file   => 'REGISTRATION.txt',
  dnrm_table    => 'registration'
);
  
 my $return = $import->connect_to_db;
 print "return [".Dumper($return)."]\n"; 
 my $table_definition = $import->load_table_definition;
 
 print "table_definition [".Dumper($table_definition)."]\n"; 
 #my @elements = $table_definition->{elements};
 print " $_->{dnrm_key} $_->{dnrm_name} $_->{type} $_->{hydstra_table}\n" for @{$table_definition->{elements}}; #[0]{required};
 #print "required [".Dumper(\@elements)."]\n";
 print "done\n";