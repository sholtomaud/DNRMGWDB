#!/usr/bin/perl -w
use FindBin qw($Bin);
use lib "$Bin/lib"; 
use DNRMGWDB;
use Data::Dumper;
use constant DB_DIR => $Bin.'/gwdb/'; 
#use constant TABLE_DEFINITION_DIR => "$Bin/../lib/DNRMGWDB/config";

use File::Temp qw(tempdir);
use Cwd;
 
my $dir = tempdir( CLEANUP => 1 );
print cwd, "\n";
chdir $dir;
print cwd, "\n";


my $think = use lib "$FindBin::Bin/../lib";


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
 print " [$_->{dnrm_key_field}] [$_->{dnrm_field}] [$_->{sqlite_type}] \n" for @{$table_definition->{elements}}; #[0]{required};
 
print "think [$think] TABLE_DEFINITION_DI [.". TABLE_DEFINITION_DIR ."]";
 
 #print "required [".Dumper(\@elements)."]\n";
 print "done\n";