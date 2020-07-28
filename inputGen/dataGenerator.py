# import the random module
import random
# define the main function
import sys


def main():
    if len(sys.argv) != 3:
        print("arg1 = number of numbers, arg2 = range of values")
        exit(0)
    random_numbers = open('inp.txt', 'w')
    qty_numbers = int(sys.argv[1])
    counter = 1
    for count in range(1, qty_numbers):
        number = random.randint(0, int(sys.argv[2]))
        if count == (qty_numbers - 1):
            random_numbers.write(str(number))
        else:
            random_numbers.write(str(number) + ", ")
        counter += 1
    random_numbers.close()
    # tell the user that the numbers have been written to the file name.
    print('Your list of random numbers have been written to the file named with ' + str(counter) + ' values')


# call the main function
main()
