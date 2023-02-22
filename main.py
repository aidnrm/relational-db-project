from db_operations import db_operations
import sys
import os
import sqlite3 as sql
import pandas as pd
import shlex

def main():

    # check if data is loaded 
    if os.path.isfile('wealth.db'):
        data_loaded = True
    else:
       data_loaded = False

    while True:
        db_data = db_operations()
        user_input = input('=>')
        #if data is not loaded 
        while data_loaded==False:
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
                    # Call helper function that explains stuff
                    helper()
                    user_input = input('=>')
                elif user_input == '':
                    print("No input. Please enter a query. Type help if you need help")
                    user_input = input('=>')
                else:
                    parse(user_input.lower())
                    user_input = input('=>')


def parse(input):

    #  ***********PARSER STUFF*************
    #changed this part from camel case to snake
    # SHLEX RETURNS A LIST OF ALL THE WORDS, WORDS IN QUOTES such as "New York" GET ADDED TO LIST AS ['New York']
    input_list = shlex.split(input)
    # We need to add user feedback after each keyword.

    #first word must be "wealthiest", "num", "networth", "nationality", "bday", "age", "most"
    # no input_list should have over 4 elements
    if input_list[0] == "exit":
        exit()
    if (len(input_list) > 4 or len(input_list)<2) :
        invalid()
    #first word is "wealthiest"
    elif input_list[0] == 'wealthiest':
        #second word must be "billionaire"
        if input_list[1] == 'billionaire':
            #third word must be [country]
            #check length
            if len(input_list) == 3:
                do_query("wealthiest billionaire [country]", input_list[2])
            else:
                invalid()
        else:
            invalid()
    #first word is "num"
    elif input_list[0] == 'num':
        #second word must be "billionaire", "country"
        if input_list[1] == "billionaire":
            #third word must be [], [country], "nationality"
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
    #first word is "networth"
    elif input_list[0] == 'networth':
        #second word must be [name]
        if len(input_list) == 2:
            do_query("networth [name]", input_list[1])
        else:
            invalid()
    #first word is "nationality"
    elif input_list[0] == 'nationality':
        #second word must be [name]
        if len(input_list) == 2:
            do_query("nationality [name]", input_list[1])
        else:
            invalid()
    #first word is "bday"
    elif input_list[0] == 'bday':
        #second word must be [date]
        if len(input_list) == 2:
            do_query("bday [date]", input_list[1])
        else:
            invalid()
    #first word is "age"
    elif input_list[0] == 'age':
        #second word is [name]
        if len(input_list) == 2:
            do_query("age [name]", input_list[1])
        else:
            invalid()
    #first word is "most"
    elif input_list[0] == 'most':
        #second word must be "billionaire", "num"
        if input_list[1] == "billionaire":    
            do_query("most billionaire", "")
        elif input_list[1] == "num":
            #third word must be billionaire
            if input_list[2] == "billionaire":
                do_query("most num billionaire", "")
            else:
                invalid()
        else:
            invalid()
    else:
        invalid()


#TODO: do query
    # query type --
    # the possible query types are...
    # wealthiest billionaire [country],
    # num billionaire, num billionaire [country], num billionaire nationality [name], num country
    # networth [name]
    # nationality [name]
    # bday [date]
    # age [name]
    # most billionaire, most num billionaire
def do_query(query_type, arg):
    con = sql.connect('wealth.db')
    cur = con.cursor()
    #found the query!
    found = True
    # TODO: Wealthiest billionaire in  specific country
    if query_type == "wealthiest billionaire [country]":
        cur.execute(f"SELECT name from richest where nationality = '{arg}' COLLATE NOCASE limit 1;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]}")

    # TODO: Number of billionaires per specific country
    elif query_type == "num billionaire [country]":
        cur.execute(f"SELECT num_billionaires FROM num_billionaires where country = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} billionaires in {arg}")

    # TODO: Num billionaires total
    elif query_type == "num billionaire":
        cur.execute(f"SELECT count(DISTINCT name) from richest;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} billionaires total")

    # TODO: Num country total
    elif query_type == "num country":
        cur.execute(f"SELECT count(DISTINCT country) from num_billionaires;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} countries total")

    # TODO: Networth of person
    elif query_type == "networth [name]":
        cur.execute(f"SELECT net_worth from richest where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]}")

    # TODO: Nationality of person
    elif query_type == "nationality [name]":
        cur.execute(f"SELECT nationality from richest where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]}")

    # TODO: Num Nationality of person - join of tables
    elif query_type == "num billionaire nationality [name]":
        cur.execute(f"SELECT num_billionaires FROM num_billionaires join richest on country = nationality where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} billionaires")

    # TODO: Birthday
    elif query_type == "bday [date]":
        cur.execute(f"SELECT name from richest where bday = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            for person in output:
                print(f"{person[0]}\n")

    # TODO: Age
    elif query_type == "age [name]":
        cur.execute(f"SELECT age FROM richest where name = '{arg}' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} years old")

    # TODO: most billionaire
    elif query_type == "most billionaire":
        cur.execute(f"SELECT country, max(billionaire_per_million) from num_billionaires;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} has {output[0][1]} billionaires per million")

    # TODO: Most num billionaire
    elif query_type == "most num billionaire":
        cur.execute(f"SELECT country, max(num_billionaires) from num_billionaires where country != 'World' COLLATE NOCASE;")
        output = cur.fetchall()
        if len(output) == 0 :
            invalid()
        else:
            print(f"{output[0][0]} has the most billionaires, {output[0][1]} billionaires")
         
    else:
        print("something went wrong!")


# TODO: Help function to explain the code 
def helper():
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
