#!/usr/bin/perl -w
use Test::More tests => 4;
use DNRMGWDB;
use FindBin qw($Bin);
use constant DB_DIR => $Bin.'/gwdb/'; 
use constant TABLE_DEFINITION_DIR => $Bin.'/DNRMGWDB/config/'; 

  my $import = DNRMGWDB->new(
    db_dir        => DB_DIR,
    db_name       => 'gwdb.db',
    import_dir    => $Bin.'/dnrmdata',
    import_file   => 'REGISTRATION.txt',
    dnrm_table    => 'registration',
    table_definition_dir =>TABLE_DEFINITION_DIR 
  );
  
 mkdir DB_DIR if (! -d DB_DIR );
 
ok(defined $import, 'DNRMGWDB->new returned something' );
ok(-d DB_DIR, 'made $Bin directory ok' );
ok($import->connect_to_db, 'import_to_sqlite()');
ok($import->load_table_definition, 'load_table_definition()');
#ok(unlink $file,'unlinking file [$file]');
#ok(rmdir $logpath,'rmdir temp [$logpath]');
