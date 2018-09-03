# Inserta datos en forma "continua" en la tabla t1

use DBI;
use DateTime;

my $dbfile = 'cached.db';
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","", { RaiseError => 1, AutoCommit => 1 });

my $sth = $dbh->prepare('INSERT INTO t1 VALUES ( ?, ?, ?, ?)');

my $DELAY=10;
my $i = 0;
my $ts;
while ( 1 ) {
    $ts = localtime();    
    my $ult_ts = DateTime->now->epoch;
    $sth->execute( $ult_ts, undef, 'valor nuevo '.$i, $ts);
    sleep rand(int($DELAY));
    print "$i : $ult_ts : valor nuevo $i \t $ts\n";
    ++$i;
}

$sth->finish();
$dbh->disconnect();
