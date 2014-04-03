
package DNRMGWDB;
use Moose;
use DBI;
use JSON;
use DateTime;
use Env;
use FindBin qw($Bin);
use File::Copy;
use Logger;
use Try::Tiny;

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

Use this module to import data from pipe "|" delimited QLD DNRM Ground Water db .txt files to Hydstra.
  
Notes:

=over 

=item 1 

This importer caches data in a provisional, dated SQLite database for further processing

=item 2 

The SQLite db is located at $Bin\db by default

=item 3 

The import also performs some provisional cleansing of the data by removing unclosed quote (") or (') characters that exist in the DNRM GWDB files making the db invalid on import

=back 

Code snippet.

  use DNRMGWDB;
  
  my $import = DNRMGWDB->new( 
    db_dir      => DB_DIR,
    db_name     => 'gwdb.db',
    import_dir  => 'C:\\temp\\dnrmdata'
  );

  $import->import_to_sqlite;
     
=cut

=head1 CONSTANTS

=head2 SUCCESS
  
  Constant used for logging the success status of the module at any step

=cut

use constant SUCCESS => 1;

=head2 FAIL
  
  Constant used for logging the fail status of the module at any step

=cut

use constant FAIL    => 0;

=head2 DEFAULT_LOGGING_DB_DIR

  Constant used creating a default logging database location

=cut

use constant DEFAULT_LOGGING_DB_DIR => $Bin.'\\db';

 has 'db_dir' => ( is => 'rw', isa => 'Str', required => 1, default => DEFAULT_LOGGING_DB_DIR); 
 has 'db_name' => ( is => 'rw', isa => 'Str', required => 1, default => 'GWDB.db'); 
 has 'import_dir' => ( is => 'rw', isa => 'Str', required => 1); 
 has 'date'  => ( is => 'rw', isa => 'Num',  default => sub { ((localtime)[5] + 1900 ). (localtime)[4] . (localtime)[3] } ); 
 has 'time'  => ( is => 'rw', isa => 'Num', default => sub { (localtime)[2] . (localtime)[1] . (localtime)[0] }); 

=head1 EXPORTS

  * import_to_sqlite()
  

=head1 SUBROUTINES/METHODS

=head2 import_to_sqlite()
  
Imports the QLD DNRM "|" delimited Ground Water Database files to a dated temp SQLite database

=cut

sub import_to_sqlite{
  my $self = shift;
  
  #Make the db dir if it doesn't exist
  mkdir ($self->db_dir) if ( ! -d $self->db_dir);
  
  try {
    my $db = $self->db_dir.$self->db_name;
    my $dbh = DBI->connect(          
        "dbi:SQLite:dbname=$db", 
        "",                          
        "",                          
        { RaiseError => 1, AutoCommit => 0},         
    ) or die print $DBI::errstr;
  }
  catch{
    my $logger = Logger->new( 
        station => 'n/a',
        status  => 'FAIL',
        keyword => 'DBICONNECT',
        script  => $0,
        errmsg  => $_
      ); 
      $logger->log;
    warn "caught error: $_";
    return FAIL;
  };
}

=skip
sub readfiles{ 
  my $d = $self->import_dir;
  
  try {
    opendir(D, "$d") || die "Can't open directory $d: $!\n";
  } 
  catch {
    warn "caught error: $_";
    return FAIL;
  };

  my @list = grep { (!/^\./) && -f "$d/$_" } readdir(D);
  closedir(D);
  return @list;
  
}  
    
  
  my $primary_key = 'PRIMARY KEY (Station, Date, Time, Keyword)';
  $dbh->do("CREATE TABLE IF NOT EXISTS LOG(
    Station   TEXT, 
    Keyword   TEXT, 
    Status    TEXT, 
    Comment   TEXT, 
    Errmsg    TEXT,
    Date      TEXT, 
    Time      TEXT, 
    Script    TEXT,
    User      TEXT,
    $primary_key
  )");
    
  my $sth = $dbh->prepare("INSERT INTO LOG VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )");
  my @values = ($self->station, $self->keyword, $self->status, $self->comment, $self->errmsg, $self->date, $self->time, $self->script, $self->user );
  $sth->execute(@values) or die return $sth->errstr;
  $dbh->commit;  
  $dbh->disconnect();
  return 1;    
}
=cut


=head1 AUTHOR

Sholto Maud, C<< <sholto.maud at gmail.com> >>

=head1 BUGS

Please report any bugs in the issues wiki.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Import

=over 4

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2014 Sholto Maud.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of Import
