import csv
import argparse
import sys
import os
from faker import Faker
from utils.common_vars import USER_DIRECTORY, DATA_DIRECTORY, FLIGHTS, BUS, TRAIN

transportation_type_list = [FLIGHTS, BUS, TRAIN]
def generate_csv_data(generation_number, transportation_type, verbose):
    try:
        if transportation_type not in transportation_type_list:
            if verbose:
                msg = 'Error in {} - Invalid transportation type'.format(transportation_type)
                print(msg)
            return False
        fake = Faker()
        Faker.seed(0)
        
        header = ['From_Country','To_Country',
            'From_City','To_City',
            'From_Date','To_Date',
            'Flight_Number','Departure','Arrival',
            'Economy','Business', 'First_Class']
        rows = []

        for _ in range(generation_number):
            from_country = fake.country()
            to_country = fake.country()
            from_city = fake.city()
            to_city = fake.city()
            from_date = fake.date_this_decade().strftime('%Y-%m-%d')
            to_date = fake.date_this_decade().strftime('%Y-%m-%d')
            flight_number = str(fake.numerify(text='F###'))
            departure = fake.time(pattern='%H:%M')
            arrival = fake.time(pattern='%H:%M')
            economy = fake.random_int(min=100, max=1000, step=100)
            business = fake.random_int(min=1000, max=2000, step=100)
            first_class = fake.random_int(min=2000, max=3000, step=100)

            rows.append([from_country,to_country, from_city, to_city,
                        from_date, to_date, flight_number, 
                        departure, arrival, economy, business, first_class])

        if not os.path.exists(DATA_DIRECTORY):
            os.makedirs(DATA_DIRECTORY)      
        if verbose:
            msg=DATA_DIRECTORY
            print(msg)
        flights_directory = os.path.join(DATA_DIRECTORY, f'{transportation_type}.csv')
        with open(flights_directory, 'w', newline='', encoding='UTF-8') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)
                
    except Exception as e:
        if verbose:
            msg = 'Error in generating the {} - {}'.format(transportation_type, e)
            print(msg)
        return False
    return True

def main():
    (generation_number,
     transportation_type,
     verbose) = check_args(sys.argv[1:])

    msg = (f' generation_number: {generation_number}\n'
           f' transportation_type: {transportation_type}\n'
           f' verbose: {verbose}\n')  
    if verbose: 
        print(msg)
    success = generate_csv_data(int(generation_number), transportation_type, verbose)
    if success:        
        if verbose:
            msg = 'Successfully generated the {}.csv file'.format(transportation_type)
            print(msg)
    else:
        if verbose:
            msg='Failed to generate the {}.csv file'.format(transportation_type)
            print(msg)
            
    return 0
   
def check_args(args=None):
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Generate flights.csv file")
    parser.add_argument(
        "-g", "--generation_number",
        help="Enter how many flights you want to generate",
        required=False,
        default='default')
    
    parser.add_argument(
        "-type", "--transportation_type",
        help="Enter the transportation type. Valid types are: {FLIGHTS}, {BUS}, {TRAIN}}",
        required=False,
        default='flights'
    )
    
    parser.add_argument(
        "-v", "--verbose",
        help="Enable verbosity",
        required=False,
        default=False,
        action='store_true')

    cmd_line_args = parser.parse_args(args)
    return (cmd_line_args.generation_number,
            cmd_line_args.transportation_type,
            cmd_line_args.verbose
            )

if __name__ == '__main__':
    main()
