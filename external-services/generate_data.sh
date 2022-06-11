#!/bin/bash

docker exec -i postgres psql -U postgres <<-EOF
    INSERT INTO inventory.products ("name",description,weight)
    select md5(random()::text) as "name",
    md5(random()::text) as description,
    floor(random()*1000000) as weight
    from generate_Series(0,1000000 - 1) id;
EOF
