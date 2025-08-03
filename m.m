// Load prelude here.

partialSum := 0;

while true do
  read line; z := eval line; // Note: we wait a bool like false to exit the loop!
  if Type(z) eq BoolElt then
    break;
  end if;
  partialSum +:= z;
end while;

partialSum;

exit; // Necessary to tell the orchestrator we are done.

