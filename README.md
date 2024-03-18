# capestone_youtube
The goal of this project is to use Python to gather and organize data from YouTube channels. It involves gathering data, such as playlists, comments, video specifics, and channel information, by utilizing the YouTube Data API.

TOOLS AND TECHNOLOGIES USED IN THE PROJECT: 

  1) GOOGLE API CLIENT: The Google API Client library is a handy toolkit for developers to connect and interact with Google's APIs. It simplifies tasks like making requests, handling authentication, and processing responses, allowing developers to easily incorporate Google services into their applications.
  
  2) PYTHON: Python is a popular interpreted programming language that is easy to learn and use. Python is the preferred language for data science because it offers a lot of libraries like NumPy, Pandas, Matplotlib, etc. These tools facilitate data manipulation, analysis, and visualization, making Python an essential tool for activities like machine learning, statistical modeling, and data exploration.
  
  3) MONGODB: MongoDB is a popular NoSQL database known for its flexibility, scalability, and ease of use. It stores data in a way similar to how you'd organize information in a document. It's great for handling lots of data and can grow as your needs increase. MongoDB is popular because it's easy to set up, works well with modern applications, and can handle complex data structures effortlessly.
  
  4) POSTGRESQL: PostgreSQL, often referred to as Postgres, is a powerful relational database management system. It is known for its reliability and ensures data is organized and safe. People like using Postgres because it's easy to understand, follows rules well, and can manage lots of information without getting confused.
  
  5) STREAMLIT: Streamlit is a user-friendly Python library for creating interactive web applications with minimal code. With Streamlit, developers can quickly turn their Python scripts into interactive web apps without needing expertise in web development. 

LIBRARIES USED IN THE PROJECT:

  1.googleapiclient.discovery
  
  2.pymongo
  
  3.psycopg2
  
  4.pandas
  
  5.streamlit

WHAT THE PROJECT DOES IS AS FOLLOWS: 

  1) Data Collection: It uses the YouTube Data API to get information about playlists, videos, title, tags, views, likes, comments, and other video-related metadata. It also 
     gathers information about the channel, including name, subscribers count, views, and comments.
  2) Data Storage: There are two locations where the gathered data is kept:
          MongoDB: Collections for channels, videos, playlists, and comments are used to store the gathered data in an organized manner.
          PostgreSQL Database: After that, the data is transferred into PostgreSQL and tables are created for simpler maintenance and querying.
  
  3) Streamlit Dashboard: The project uses Streamlit to create a user-friendly interface where the users can:
  
    * Enter the ID of a YouTube channel to start gathering data.
    * Examine the gathered data in a tabular manner.
    * Choose and run SQL queries to get particular data out of the database.
 4) SQL querying: When a user chooses a pre-defined question from a drop-down menu, the system runs the relevant SQL queries to return the appropriate responses.


