

my @colvals=(1, 2, 'dos', 10, localtime().'', 99, -5, 'uno', 77777.0564);

my $tojson = '{ ';
for ( my $i=0; $i<scalar(@colvals); ++$i) {
    $tojson .= "\'$i\' => \'$colvals[$i]\'\,";
}
chomp($tojson);
$tojson .= ' }';

print $tojson;