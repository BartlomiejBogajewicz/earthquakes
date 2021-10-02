In this task I wanted to visualize the occurrence of earthquakes. To do that I have used databricks on Azure to get the data from https://earthquake.usgs.gov/ API. Because the data from API had only the coordinates 
and one of my goals was to group the number of earthquakes by country I needed somehow to map these coordinates. I used the data from http://download.geonames.org/export/dump/ to get 12 milion coordinates with their 
location (country,nearest city). Later on I mapped the coordinates of earthquakes to the coordinates from mapping file to get the country where the earthquake happen.
To visualize the data I connected Power Bi to databricks. Below you can see the results from 20.09.2021 to 30.09.2021:

![main](https://github.com/BartlomiejBogajewicz/earthquakes/blob/master/main_screen.PNG)
![usa](https://github.com/BartlomiejBogajewicz/earthquakes/blob/master/USA.PNG)
![greece](https://github.com/BartlomiejBogajewicz/earthquakes/blob/master/Greece.PNG)

We can clearly see that on west coast of North America and east of Asia tectonic plates overthrust. Also in this time period unusual tectonic activity was recorded in Greece.