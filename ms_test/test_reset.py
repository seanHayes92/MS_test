# Quick script to reset the test
import os, shutil

if __name__ == '__main__':

    try:
        os.remove('exercise_database.db')

        shutil.move('out/ms_exercise_example.csv','in/ms_exercise_example.csv')

    except FileNotFoundError:

        print("Test already set up")