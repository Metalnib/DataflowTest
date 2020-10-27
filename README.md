#Dataflow Playground

Bounded Capacity and back-pressure
Data-flow blocks work as tanks of liquid: if the lower tank is full the higher one will start to fill.If the lower one has infinite volume the higher tanks will not be filled ever.
To configure back-pressure we have to set the capacity of every block in a chain. If some intermediate block has less flow than lower blocks it will be the bottleneck and act as the source of back-pressure (just like a valve).

This simple project demonstrate visually this data flows. 