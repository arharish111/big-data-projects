G = load '$G' using PigStorage(',') as ( x:long, y:long );
C = foreach G generate y,1L;
R = group C by y;
S = foreach R generate group,COUNT($1);
O = order S by $1 desc;
store O into '$O' using PigStorage(',');