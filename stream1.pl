#

my $DEL     = 5000;   # Milisecs entre lecturas (5000 = 5 segundos) 
my $last_id = getlastid();  # Ultimo Id Ingresado en el Destino

my $origen  = ...;      # Ej. SQL Server
my $destino = ....;     # Ej. Cassandra

my $SQL_Origen = "";

my $rrows;
my $last_in;
while ( 1 ) {
    $rrows= execSQL($last_id); # Lee filas desde el Origen
    $last_id = lastid($rrows); 
    $last_in = inyect($rrows); # Ultimo Inyectado
    if ( $last_in ) {
        persist($last_in);
    }
    sleep $DEL;
}

# Inyecta en el destino
sub inyect {
    1;
}

# Ejecuta el SQL
sub execSQL {
    my $last_id = shift;
    1;
}

# Ultimo inyectado
sub lastid {
    1;
}

# Graba el ultimo insertado en un archivo
sub persist {
    1;
}

sub getlastid {
    my $last_id;
    # lee last_id;
    die "$0 : No pude recuperar el ultimo id\n" if ( ! $last_id );
    return $last_id;
}


