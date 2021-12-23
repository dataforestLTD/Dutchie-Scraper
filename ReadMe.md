The various hashes used in the GraphQL searches will need to be updated from time to time.
To find the new hashes, you will need to find the XHR requests made during a search for dispensaries by distance, as well as the XHR request made when clicking on a store's menu.
If you look at the code where the hashes are added to URL strings, you can pretty easily figure out the requets you'll be looking for in the inspector.
I will also be keeping the hashes updated when I can.

The Weekly Search, used to add new dispensaries to the data base to be scrapped, currenty is giving me issues when trying a huge search distance like 35,000.
You may need to do a few smaller searches with specific location parameters to populate a data base with the dispensaries you want to scrape.
