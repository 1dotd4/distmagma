load "0.m";

read workerId; // Useful to log correctly.

while true do
  read line; z := eval line;
  _:=POpen("sleep " cat IntegerToString(1+Random(2)), "r");
  printf "%o\n\n", z*2 + Random(10); // Note: double new line to signal end!
end while;
