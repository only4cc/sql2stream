# Inserta datos en forma "continua" en la tabla t1

use DBI;
use DateTime;

my $dbfile = 'cached.db';
my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","", { RaiseError => 1, AutoCommit => 1 });

my $sth = $dbh->prepare('INSERT INTO t1 VALUES ( ?, ?, ?, ?)');

my $DELAY=shift ||5;
my $i = 0;
my $ts;
my $col2;
while ( 1 ) {
    $ts = localtime();    
    my $ult_ts = DateTime->now->epoch;
    $col2 = 'valor nuevo '.rand(10000);
    $sth->execute( $ult_ts, undef, $col2, $ts);
    sleep rand(int($DELAY));
    print "$i : $ult_ts : valor nuevo $col2 \t $ts\n";
    ++$i;
}

$sth->finish();
$dbh->disconnect();
