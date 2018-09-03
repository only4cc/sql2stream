# Lee datos en forma continua desde una tabla (t1)
# ts en segundos formato Epoch
# Ej. --> "SELECT * FROM t1 WHERE  strftime('%s','now') > $lastime ";
#
# Comporamiento dual (server, one-shot) - $SERVER 1 : Si | 0 : No
#
# Falta: logging (inicio, errores, fin)
#

use strict;
use warnings;
use DBI;
use DateTime;
use Storable;
use Config::Tiny;
use Try::Tiny;

# Server o on-shot (por una sola vez)
my $SERVER = shift;  # 1 : Si | 0 : No
my $MODE= ($SERVER == 1 ? "Server" : "Una-vez");

# Lee configuracion
my $configfilename = shift || 'sql2ls.conf';
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( $configfilename );
my $version     = $Config->{_}->{version};
my $DELAY       = $Config->{_}->{delay} || 5;
my $persistfile = $Config->{_}->{persist_filename};
my $logfile     = $Config->{_}->{log_filename};
my $SQLQUERY    = $Config->{origen}->{sql_query};
my $TS_COLNAME  = $Config->{origen}->{timestamp_col_name};

# Logging inicial
print "Iniciando $0 - ver. $version | Modo: $MODE ...\n";
print "delay              : $DELAY [s]\n";
print "sql_query          : $SQLQUERY\n";
print "timestamp_col_name : ${TS_COLNAME}\n";
print "persist_filename   : $persistfile\n";
print "log_filename       : $logfile\n";

$SQLQUERY .= " AND $TS_COLNAME > ?";
print "SQL                : $SQLQUERY\n";

# Logging
if ( $SERVER  ) {
  print "logeando ...\n";
}

# Coneccion a la fuente de datos
my $user='';
my $password='';

my $dbfile = 'cached.db';
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","", { RaiseError => 1})
                or die $DBI::errstr;

# Conexion x ODBC
#my $dbh = DBI->connect('dbi:ODBC:DSN=mydsn', $user, $password, { RaiseError => 1}) 
#                or die $DBI::errstr;

# Obtiene ultimo timestamp extraido 
my $lastime;
try {
  my $hashref = retrieve($persistfile);
  $lastime = $$hashref;
} catch {
  $lastime = 0;
  #die "error: $_"; # not $@
};
print "lastime            : ".$lastime."\n";


# Main
while ( 1 ) {
    my $sth = $dbh->prepare( $SQLQUERY );
    $sth->bind_param( 1, $lastime );
    $sth->execute();
    while (my $row = $sth->fetchrow_arrayref()) {
        $lastime = DateTime->now->epoch;
        print "last [ $lastime ] : fila: ". join(',',@$row). "\n"; # @$row[0] @$row[1] @$row[2]\n";
        store \$lastime, $persistfile;
    }
    $sth->finish();
    print localtime().": esperando nuevas filas, posteriores a [ $lastime ] ...\n";
    sleep $DELAY;
    exit if ( ! $SERVER ); 
}


$dbh->disconnect();

