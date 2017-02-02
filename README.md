# 7-deadly-sins
25pts-dc-project
In the last semester for my 25-pts Distributed Computing Project, I built a system, using Twitter data to identify and explore a range of human emotions with focus on Dante's seven deadly sins. It also developed targeted machine learning/classification approaches that build on natural language processing libraries to identify social media language usage. The system can be mainly divided into four parts:
1.Data collecting. 2. Data preprocessing. 3. Synsets generating and sentiment classification 4. Statistic and web front-end demo
For data collecting part, I took use of the Twitter search API and the data from Australia Urban Research Infrastructure Network (AURIN) and the total data amount is more than 16.25 GB and I used MongoDB to store them. For preprocessing data, I mainly normalized the data sets and distributed tweets with geo info into different districts to accomplish the GIS mapping tasks. And for the third part, I mainly combined the twitter data with the Brown corpus in NLTK library and used the algorithm PMI to calculate the distance between each words pair to generate and expand the sentiment seeds set as my our sentiment corpus. Based on that, the results of the preprocess will be classified into seven categories. Finally, the statistic work was demonstrated via a front-end website. The web-based front end is designed for demonstrating the data. It mainly used HTML, CSS, JavaScript, JQuery combined with some JavaScript tools like Google fusion table, amCharts and CARTO platform. Here is the link of my demo video.
