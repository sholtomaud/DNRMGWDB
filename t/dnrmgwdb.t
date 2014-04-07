#!/usr/bin/perl -w
use Test::More tests => 65;
use Data::Dumper;
use DNRMGWDB;
use FindBin qw($Bin);
use constant DB_DIR => $Bin.'/gwdb/'; 
use constant DNRM_DIR => $Bin.'/dnrmdata/'; 
use constant TABLE_DEFINITION_DIR => $Bin.'/DNRMGWDB/config/'; 

mkdir DB_DIR if (! -d DB_DIR );
ok(-d DB_DIR, 'made $Bin directory ok' );

my $dnrm_dir = DNRM_DIR;
print "bin [$Bin] dnrm dir [$dnrm_dir]\n";
opendir(DIR, $dnrm_dir);
ok(DIR, 'odendir DIR directory ok' );

my @files = grep { $_ ne '.' && $_ ne '..' } readdir DIR;
closedir DIR;

print "return [".Dumper(\@files)."]\n";

foreach my $dnrm_table (@files){
#while (my $dnrm_table = readdir(DIR)) {
  print "file [$dnrm_table]\n";
  my ($table,$file_ext) = split(/\./,$dnrm_table);
  my $import = DNRMGWDB->new(
    db_dir               => DB_DIR,
    db_name              => 'gwdb.db',
    import_dir           => $Bin.'/dnrmdata',
    import_file          => $dnrm_table,
    dnrm_table_config    => 'registration', #$table,
    table_definition_dir => TABLE_DEFINITION_DIR 
  );

  ok(defined $import, 'DNRMGWDB->new returned something' );
  ok($import->connect_to_db, 'import_to_sqlite()');
  ok($import->load_table_definition, 'load_table_definition()');

}



#ok(unlink $file,'unlinking file [$file]');
#ok(rmdir $logpath,'rmdir temp [$logpath]');
