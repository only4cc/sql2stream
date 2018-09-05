use strict;
use warnings;
use DBI;
 

#my $database='navegacion';
my $database='otrs';
#my $hostname='172.17.233.64';
my $hostname='10.34.16.111';  # OTRS
#my $password='aLienigena.16';
my $password='password';
my $user    = 'jtrumper';

my $dsn = "DBI:mysql:database=$database;host=$hostname"; #;port=$port";
my $dbh = DBI->connect($dsn, $user, $password, { RaiseError => 1, AutoCommit => 1 });

my $ts_from = '(NOW() - INTERVAL 30000 SECOND)'; #'now()'; #'2018-09-04 17:00:00'";  
my $SQL = "SELECT create_time, ticket_id, owner_id, name from  ticket_history where create_time >= ".$ts_from ." LIMIT 20";
print "Ejecutando: $SQL\n";

my $sth = $dbh->prepare($SQL);
$sth->execute();
my $i=0;
while (my $ref = $sth->fetchrow_hashref()) {
    print "row [$i]: $ref->{'ticket_id'}, $ref->{'name'}, $ref->{'owner_id'}\n";
    ++$i;
}
$sth->finish();

$dbh->disconnect();
