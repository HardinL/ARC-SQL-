# How to Install

## Using Pycharm(Easiest)

Download PyCharm from the JetBrains website. Use this link: https://www.jetbrains.com/help/pycharm/managing-dependencies.html to install the required packages for this project in the file requirements.txt

## Using Ubuntu/WSL

Create a virtual environment(not required but good practice). Once you activate the virtual environment, in your project folder, run

```bash
pip install -r requirements.txt
```

# Setup Database

You need to install mysql-server. Follow any web tutorial for the same. Once you have it setup you should be able to run

```bash
sudo mysql < database.sql
```
After this you need to update the constants.py file to the username and password for mysql.
# How to Run Queries Only

You can run the file "queries.py" directly from the terminal using "python3  queries.py" or by clicking the run button on PyCharm. The file has a sample query provided. To evaluate one of your queries, scroll to the last two lines of the file.

```python
cursor.execute(query1)
print(cursor.fetchall())
```
Here replace sample_query in the second line with the query you want to evaluate and re-run the file. By default, the file will run the sample_query as shown.

# How to run test framework

To run the test framework fill in your expressions in the file "queries.py"(don't forget to uncomment the expression by removing the leading "#") and then run "python3 run_tests.py"
This will pass your queries against the sample database and return Errors or pass values depending on the correctness of your query. Note that passing a test case locally does not mean it will pass on Gradescope against the hidden dataset as your query could be hardcoded or have some other inaccuracy.

# Submitting on Gradescope

You only need to submit the queries.py file to Gradescope.