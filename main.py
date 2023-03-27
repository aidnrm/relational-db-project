from db_operations import db_operations
import os
import sqlite3 as sql
import shlex

def main():
    """
    This function provides a command-line interface for querying data from a SQLite database.

    The function starts by checking if a file named 'wealth.db' exists in the current directory. 
    It then enters into an infinite loop, where it accepts user inputs. 
    
    If the data is not loaded, the user must type 'load data' to load the data from the file. 
    If the data has not been loaded, the user is prompted to either type 'help' for more information,
    'exit' to quit the program, or 'load data' to load data into the database. 

    If the user types an empty string, the function prompts the user to enter a query. 
    Otherwise, the function calls the parse function to process input and execute the query. 
    The function continues to prompt the user for input until the user types 'exit'.
    """
    # check if data is loaded 
    if os.path.isfile('wealth.db'):
        data_loaded = True
    else:
       data_loaded = False

    while True:
        db_data = db_operations()
        user_input = input('=>')

        # while data is not loaded
        while data_loaded == False:
            if user_input.lower() == "help":
                helper()
                user_input = input('=>')
            elif user_input.lower() == "exit":
                exit()
            elif user_input.lower() != 'load data':
                print ("You need to load the data. Type load data")
                user_input = input('=>')
            elif user_input.lower() == 'load data':
                db_data.load_data()
                data_loaded = True
                print('loading')

        #if data is loaded
        if user_input.lower() == 'load data' and data_loaded == True:
            print('Data loaded.')
        else: 
            #query hasn't been found
            found = False
            while not found:
                if user_input.lower() == 'help':
                    helper()
                    user_input = input('=>')
                elif user_input == '':
                    print("No input. Please enter a query. Type help if you need help")
                    user_input = input('=>')
                else:
                    parse(user_input.lower())
                    user_input = input('=>')


def parse(input):
    """
    This function parses user input and returns a list of words in the input. 
    Words enclosed in quotes are treated a single element in the list. The function also
    validates the input against pre-defined commands and executes based on the input
     
    Parameters:
     input (str): Represents user's input
      
    Returns:
     A list of words for input to the do_query functions query_type param.
     Matches the input to the predefined commands, do_query function is then executed with as the arg param. 
    """
    # Returns a list of all the words, words in quotes such as "New York" are added to a list as ['New York']
    input_list = shlex.split(input)

    if input_list[0] == "exit":
        exit()
    
    if (len(input_list) > 4 or len(input_list)<2) :
        invalid()
    
    # First word is "wealthiest", second is "billionaire", third is "country"
    elif input_list[0] == 'wealthiest':
        if input_list[1] == 'billionaire':
            if len(input_list) == 3:
                do_query("wealthiest billionaire [country]", input_list[2])
            else:
                invalid()
        else:
            invalid()
    
    #first word is "num", second is "billionaire" or "country", third is 
    elif input_list[0] == 'num':
        if input_list[1] == "billionaire":
            if len(input_list) == 2:
                do_query("num billionaire", "")
            elif len(input_list) == 3:
                do_query("num billionaire [country]", input_list[2])
            elif input_list[2] == "nationality" and len(input_list) == 4:
                do_query("num billionaire nationality [name]", input_list[3])
            else:
                invalid()
        elif input_list[1] == "country":
            do_query("num country", "")
        else:
            invalid()

    #first word is "networth", second is "name"
    elif input_list[0] == 'networth':
        if len(input_list) == 2:
            do_query("networth [name]", input_list[1])
        else:
            invalid()

    #first word is "nationality", second is "name"
    elif input_list[0] == 'nationality':
        if len(input_list) == 2:
            do_query("nationality [name]", input_list[1])
        else:
            invalid()

    #first word is "bday", second is "date"
    elif input_list[0] == 'bday':
        if len(input_list) == 2:
            do_query("bday [date]", input_list[1])
        else:
            invalid()

    #first word is "age", second word is "name"
    elif input_list[0] == 'age':
        if len(input_list) == 2:
            do_query("age [name]", input_list[1])
        else:
            invalid()

    #first word is "most", second is "billionair" or "num"
    elif input_list[0] == 'most':
        if input_list[1] == "billionaire":    
            do_query("most billionaire", "")
        elif input_list[1] == "num":
            if input_list[2] == "billionaire":
                do_query("most num billionaire", "")
            else:
                invalid()
        else:
            invalid()
    else:
        invalid()



def do_query(query_type, arg):
    """
    This do_query function takes query_type and arg, and performs queries based on the input.
    do_query connects to the wealth.db, performing the queries based on the input. 
    
    Parameters:
     query_type (str): Type of query to be performed
     arg (str): The argument to be passed to the query
    """
    
    con = sql.connect('wealth.db')
    cur = con.cursor()
    
    if query_type == "wealthiest billionaire [country]":
        cur.execute(f"SELECT name from richest where nationality = '{arg}' COLLATE NOCASE limit 1;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]}")

    elif query_type == "num billionaire [country]":
        cur.execute(f"SELECT num_billionaires FROM num_billionaires where country = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} billionaires in {arg}")

    elif query_type == "num billionaire":
        cur.execute(f"SELECT count(DISTINCT name) from richest;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} billionaires total")

    elif query_type == "num country":
        cur.execute(f"SELECT count(DISTINCT country) from num_billionaires;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} countries total")

    elif query_type == "networth [name]":
        cur.execute(f"SELECT net_worth from richest where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]}")

    elif query_type == "nationality [name]":
        cur.execute(f"SELECT nationality from richest where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]}")

    elif query_type == "num billionaire nationality [name]":
        cur.execute(f"SELECT num_billionaires FROM num_billionaires join richest on country = nationality where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} billionaires")

    elif query_type == "bday [date]":
        cur.execute(f"SELECT name from richest where bday = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            for person in output:
                print(f"{person[0]}\n")

    elif query_type == "age [name]":
        cur.execute(f"SELECT age FROM richest where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} years old")

    elif query_type == "most billionaire":
        cur.execute(f"SELECT country, max(billionaire_per_million) from num_billionaires;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} has {output[0][1]} billionaires per million")

    elif query_type == "most num billionaire":
        cur.execute(f"SELECT country, max(num_billionaires) from num_billionaires where country != 'World' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} has the most billionaires, {output[0][1]} billionaires")
         
    else:
        print("something went wrong!")


def helper():
    ''' Function prints how to use this program when called. '''

    print("Welcome to the Wealth Database -- a database about the wealthiest people in the world.\n")
    print("The keywords in this system are: \"country\", \"wealthiest\", \"billionaire\", \"bday\" , \"nationality\", \"age\", \"most\", \"num\"")
    print("Please put quotation marks around names and country names and dates and make sure to capatalize (ex: \"Elon Musk\")\n")
    print("Use these commands to access the information you want: ")
    print("wealthiest billionaire [Country] -- returns the wealthiest billionaire from a country")
    print("num billionaire [Country] -- returns the number of billionaires in a country")
    print("num billionaire -- returns number of billionaires in the dataset")
    print("num country -- returns the number of countries in the dataset")
    print("networth [Name] -- returns given billionaire's networth")
    print("nationality [Name] -- returns given billionaire's nationality")
    print("num billionaire nationality [Name] -- returns number of billionaires in the country of specified person")
    print("bday [Date (dd-mmm-yy)] -- returns all billionaires with given birthday")
    print("age [Name] -- returns age of given billionaire")
    print("most billionaire -- returns country with highest number of billionaires per million people")
    print("most num billionaire -- returns country with the most billionaires")


def invalid():
    print("Invalid input. Please write \"help\" for more information.")


if __name__ == "__main__":
    main()
