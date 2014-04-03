#!/usr/bin/perl -w
use Test::More tests => 3;
use DNRMGWDB;
use FindBin qw($Bin);
use constant DB_DIR => $Bin.'\\gwdb'; 

  my $import = DNRMGWDB->new(
    db_dir      => DB_DIR,
    db_name     => 'gwdb.db',
    import_dir  => $Bin.'\\dnrmdata'
  );
  
 mkdir DB_DIR if (! -d DB_DIR );
  
ok(defined $import, 'DNRMGWDB->new returned something' );
ok(-d DB_DIR, 'made $Bin directory ok' );
ok($import->import_to_sqlite(), 'import_to_sqlite()');
#ok($logger->log_hash  , 'dev_log()');
#ok(unlink $file,'unlinking file [$file]');
#ok(rmdir $logpath,'rmdir temp [$logpath]');
