# Lesson Plan: Spark Up a Conversation - DataFrames and Spark SQL
## Overview

Today's class will introduce students to how to join two data structures in Spark. 

Slack out some of the following helpful links, and encourage students to conduct research and review supplementary resources outside of class.

  * [DataFrames API Guide](https://spark.apache.org/docs/2.4.7/sql-programming-guide.html)
  * [PySpark DataFrames API Documentation](https://spark.apache.org/docs/2.4.7/api/python/pyspark.sql.html)
  * [StackOverflow: Writing SQL vs using Dataframe APIs in Spark SQL](https://stackoverflow.com/questions/45430816/writing-sql-vs-using-dataframe-apis-in-spark-sql)
  * [Superhero Dataset from Kaggle](https://www.kaggle.com/claudiodavi/superhero-set)

## Learning Objectives
By the end of this class, students will be able to:

  * Understand how the DataFrames and SQL APIs connect to Spark.
  * Write queries to join data structures with the DataFrames API and the SQL API
  * Debug queries written with the DataFrames and SQL APIs
  * Be able to compare the DataFrames and SQL APIs and discuss pros and cons

## Instructor Prep

<details>
  <summary><strong>Instructor Notes</strong></summary>

  * Provide the folders with today's activities to students before class. Instruct them to upload the entire Activities folder to the Google Drive folder that they've been using for class so far.
  *  Make sure that you upload the Activities folder to Google Drive as well. Run through the activities so you are familiar with the code and can anticipate any issues the students may run into.
  * Before class, go to http://www-us.apache.org/dist/spark/ and look for the latest version of spark 2.0. Update the `spark_version` variable accordingly in the first block of code for each activity. You may want to give students a heads up if the version has changed.
  * Each notebook starts with the same 3 blocks of code that you need to run to set up Spark in that particular Google Colab notebook. The first block of code to install Spark and its dependencies can take up to a minute to run. So when you open each notebook, immediately run the first cell. While you wait for the code to run, you can give a high-level overview of the activity and introduce the concepts.

</details>

<details>
  <summary><strong>Class Slides and Time Tracker</strong></summary>

The slides for this lesson can be viewed on Google Drive here: [Lesson Slides](https://docs.google.com/presentation/d/1Q-PVlZOfb67LuNnJWJ0eq8b5zTPO0ynpTNoYal6mYBI/edit?usp=sharing).

The time tracker for this lesson can be viewed here: [Time Tracker](https://docs.google.com/spreadsheets/d/1cRuHaY-ELLqW2cY9ltv_WCC4MzHwAd79Vx30Tn7nuVE/edit?usp=sharing)

</details>

- - -

## Class Activites

<details>
  <summary><strong>📣 1. Instructor  Do: Welcome (0:10)</strong></summary>

  Welcome students to class. Explain that today we will be discussing Spark SQL and how to join data structures in two different ways: using SQL and using the DataFrames API.
  
  To get students engaged, and also to help you understand your students' experience so you can tailor the rest of the class, ask the following questions.

  * Who has used SQL?
    * Ask what their main use cases are.
    * What SQL client do they use to run SQL queries?
    * Do they export the data and manipulate it somewhere else?
  * Who has used pandas before? For those who are not familiar, you can explain that pandas is a popular Python library used for data analysis.
    * Ask about the latest project they completed with pandas.
    * Ask if they prefer manipulating data in pandas DataFrames in Python or with SQL.
  
  Reassure students that they can use the skills they have built up with SQL or pandas DataFrames with Spark, but with Spark they can analyze much larger datasets!

</details>

- - -

<details>
  <summary><strong>📣 2. Instructor Do: Intro to Spark SQL (0:10)</strong></summary>

  Open the lesson slides.
  #FIXME finish slides and insert notes here

</details>

- - -

<details>
  <summary><strong>✏️ 3. Instructor Do: Compare DataFrames and SQL APIs (0:10)</strong></summary>

  **Corresponding Activity:** [01-Ins_SQL_and_DataFrames](Activities/01-Ins_SQL_and_DataFrames)

  **File:** [datetime_basics.ipynb](Activities/01-Ins_SQL_and_DataFrames/Solved/)

  * Make sure you looked up the latest version of spark 2.0 and updated all of the notebooks for today's activities per the instructions in [Instructor Prep](#instructor-prep).
    * #FIXME insert image of first code block
  * Run the first block of code. While it is running (it can take up to a minute), remind students that we need to include this code in every Google Colab notebook where we want to run Spark. Because Google Colaboratory allows you to execute code on Google's cloud servers, each time we open a new notebook we are essentially starting with a blank slate, so we need to install our dependencies.
  * Run the next two blocks of code and explain to students that we are setting up and running a Spark session.
    * #FIXME insert image of code block 2-3
  * Next we will import the data. Run this block of code. Click on the "Choose Files" button. Navigate to and select the `heroes_information.csv`.
    * You can mention that the data was obtained from Kaggle using [this link](https://www.kaggle.com/claudiodavi/superhero-set).
    * #FIXME insert image of code block 4 and file upload
  * Now that we have imported the data file, let's load the data into a Spark DataFrame.
    * Mention to students that Spark can also load in data from JSON, CSV, and text files, and more importantly, databases.
    * #FIXME insert image of code block 5
  * Next we create a view from the newly-created DataFrame so that we can query it in SQL.
    * #FIXME insert image of code block 6
  * Now let's compare the performance of writing a query in SQL versus with the DataFrames API. Here we are writing a query to count the number of superheroes by hair color.
    * #FIXME insert image of code block 7
  * Next we write the same query with the DataFrames API.
    * Point out that we need to import `col` in order to use the `orderBy()` function.
    * You can mention that this code can be written all on one line. Some people prefer to keep things in as few lines as possible, while others prefer line breaks for readability.
    * Show that this approach is a bit more forgiving with the ordering. For example, you can put the `limit()` clause before the `orderBy()` clause and you will get the same result. If you did the same ordering in SQL, you would get an error.
    * Point out that both methods return the same results.
    * #FIXME insert image of code block 8
  * Finally, let's look at the physical plan for each approach by using `explain()`. This is how Spark will execute each query. Point out that the physical plans are identical between the two approaches.
    * Students might be familiar with the similar `EXPLAIN` clause in SQL.
    * It's not important to dive into the details of the physical plan right now. Later on it will be helpful for tuning our Spark queries, but for now we want to notice that using the DataFrames or SQL API produces the exact same performance.
    * #FIXME insert image of code block 9
  * That's it! Now that we can see the two approaches have the same performance, you can use whichever method you and your team feel most comfortable with.
  
</details>

- - -

<details>
  <summary><strong>✏️ 4. Student Do: Superheroes in SQL (0:15)</strong></summary>

**Corresponding Activity:** [02-Stu_](Activities/02-Stu_Superheroes_SQL)

In this activity, students will join across two different datasets using the SQL API.

**Instructions:**
  * [README.md](Activities/02-Stu_Superheroes_SQL/README.md)
    * #FIXME write instructions in README.md file

**File:**
  *[superheroes_in_sql.ipynb](Activities/02-Stu_Superheroes_SQL/Unsolved/superheroes_in_sql.ipynb)
    #FIXME make Unsolved version of this activity

</details>

- - -

<details>
  <summary><strong>⭐ 5. Instructor Do: Review Superheroes in SQL (0:15)</strong></summary>

**File:**
  *[superheroes_in_sql.ipynb](Activities/02-Stu_Superheroes_SQL/Solved/superheroes_in_sql.ipynb)

#FIXME add notes here

</details>

- - -