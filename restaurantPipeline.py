import csv
import logging
import multiprocessing
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from multiprocessing import Event

from multiprocessing.process import current_process

import pandas


class RestaurantConsts:
    DOUGH_CHEFS_NUM = 2
    DOUGH_DELAY = 7
    TOPPINGS_CHEFS_NUM = 3
    TOPPING_DELAY = 4
    TOPPINGS_PER_WORKER = 2
    OVENS_NUM = 1
    OVEN_DELAY = 10
    WAITERS_NUM = 2
    WAITERS_DELAY = 5
    END = 'End'

    ORDER_NAME ='OrderName'
    ORDER_TOPPINGS = 'Toppings'
    START_TIME ='StartTime'
    END_TIME = 'EndTime'
    TOT_TIME = 'PreparationTime'


class Toppings:
    Mushroom = 'Mushroom'
    ExtraCheese = 'Extra-Cheese'
    Sausage = 'Sausage'
    Onion = 'Onion'
    BlackOlives = 'Black-Olives'


class Order(object):
    def __init__(self, name, toppings = []):
        self.name = name
        self.toppings = toppings
        self.start_time = datetime.now()
        self.end_time = None

    def set_end_time(self, end_time):
        self.end_time = end_time


class Pipe(object):
    def __init__(self, in_queue, out_queue, resource_num = 1, delay_at_pipe = 5, unit_per_resource = 1):
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.resource_num = resource_num
        self.delay_at_pipe = delay_at_pipe
        self.unit_per_resource = unit_per_resource
        self.name= type(self).__name__
        self.event_stop = Event()
        self.processes = None

    def manage(self):
        processes = [multiprocessing.Process(target=self.worker, name=self.name.replace('Pipe', '') + '_' + str(i)) for i in range(self.resource_num)]
        for proc in processes:
            proc.start()
        self.processes = processes

    def stop(self):
        for proc in self.processes:
            proc.terminate()

    def worker(self):
        raise NotImplementedError()


class DoughPipe(Pipe):

    def __init__(self, in_queue, out_queue, resource_num, delay_at_pipe):
        super().__init__(in_queue, out_queue, resource_num, delay_at_pipe)

    def worker(self):
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', datefmt='%H:%M:%S', filename=os.path.join('logs',current_process().name+'.log'))
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)

        while True:
            for order in iter(self.in_queue.get, None):
                logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, order.name, 'START', time.strftime("%H:%M:%S", time.localtime())))
                time.sleep(self.delay_at_pipe)
                cnt = 0
                for topping in order.toppings:
                    cnt += 1
                    self.out_queue.put((order.name, topping, cnt, len(order.toppings)))
                if len(order.toppings) == 0:
                    self.out_queue.put((order.name, None, 0, len(order.toppings)))
                logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, order.name, 'END', time.strftime("%H:%M:%S", time.localtime())))


class ToppingsPipe(Pipe):
    def __init__(self, in_queue, out_queue, resource_num, delay_at_pipe, unit_per_resource):
        super().__init__(in_queue, out_queue, resource_num, delay_at_pipe, unit_per_resource)

    def worker(self):
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', datefmt='%H:%M:%S', filename=os.path.join('logs', current_process().name+'.log'))
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)
        items_list = []
        while True:
            while not self.in_queue.empty():

                while len(items_list)<2 :
                    if not self.in_queue.empty():
                        cur_name, cur_topping, ind, tot_topping = self.in_queue.get()
                        if tot_topping == 0:
                            self.out_queue.put((cur_name, cur_topping, ind, tot_topping))
                            logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, cur_name, 'SKIP TOPPINGS', time.strftime("%H:%M:%S", time.localtime())))
                            continue
                        if ind == 1:
                            logging.debug(
                                'Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, cur_name, 'START',
                                                                                     time.strftime("%H:%M:%S",
                                                                                                   time.localtime())))
                        items_list.append((cur_name, cur_topping, ind, tot_topping))
                    else:
                        break
                time.sleep(self.delay_at_pipe)

                for cur_name, cur_topping, ind, tot_topping in items_list:
                    if ind == tot_topping:
                        logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, cur_name, 'END', time.strftime("%H:%M:%S", time.localtime())))

                    self.out_queue.put((cur_name, cur_topping, ind, tot_topping))


class OvenPipe(Pipe):

    def worker(self):
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', datefmt='%H:%M:%S', filename=os.path.join(os.getcwd(),'logs',current_process().name+'.log'))
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)
        orders_dict = defaultdict(list)
        while True:
            for item in iter(self.in_queue.get, None):
                cur_name, cur_topping, ind, tot_topping = item
                if cur_topping == None:
                    orders_dict[cur_name] = []
                else:
                    orders_dict[cur_name].append(cur_topping)
                if len(orders_dict[cur_name]) == tot_topping:
                    logging.debug( 'Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, cur_name, 'START', time.strftime("%H:%M:%S", time.localtime())))

                    # self.set_step_log_msg()
                    time.sleep(self.delay_at_pipe)
                    self.out_queue.put((cur_name, orders_dict[cur_name]))
                    logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, cur_name, 'END', time.strftime("%H:%M:%S", time.localtime())))

                    orders_dict.pop(cur_name)


class WaitersPipe(Pipe):

    def worker(self):
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(message)s', datefmt='%H:%M:%S', filename=os.path.join('logs', current_process().name+'.log'))
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)
        while True:
            for item in iter(self.in_queue.get, None):
                logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, item[0], 'START', time.strftime("%H:%M:%S", time.localtime())))

                time.sleep(self.delay_at_pipe)
                self.out_queue.put(item)
                logging.debug('Process: {}, Order : {}, Step: {}, Time: {}'.format(current_process().name, item[0], 'END', time.strftime("%H:%M:%S", time.localtime())))


class ManageRestaurant(object):

    def __init__(self, orders):
        self.orders = orders
        self.in_orders_queue = multiprocessing.Queue()
        self.orders_producer = multiprocessing.Process(target=self.set_orders, name='manage orders')
        self.set_pipes()
        self.start_time = datetime.now()
        self.set_log_dir()
        self.set_report()


    def set_log_dir(self):
        self.log_dir_path = os.path.join(os.getcwd(), 'logs')
        if not os.path.exists(os.path.join(os.getcwd(), 'logs')):
            os.makedirs(os.path.join(os.getcwd(), 'logs'))
        else:
            for filename in os.listdir(self.log_dir_path):
                file_path = os.path.join(self.log_dir_path, filename)
                os.remove(file_path)

    def set_report(self):
        if not os.path.exists(os.path.join(os.getcwd(), 'reports')):
            os.makedirs(os.path.join(os.getcwd(), 'reports'))

    def set_orders(self):
        for order in self.orders:
            self.in_orders_queue.put(order)

    def start_manage(self):
        self.orders_producer.start()
        self.dough_to_toppings_pipe.manage()
        time.sleep(RestaurantConsts.DOUGH_DELAY)
        self.toppings_to_oven_pipe.manage()
        time.sleep(RestaurantConsts.TOPPING_DELAY)
        self.oven_to_waiters_pipe.manage()
        time.sleep(RestaurantConsts.OVEN_DELAY)
        self.waiters_pipe.manage()

    def set_pipes(self):
        # in_orders_queue == dough_in
        topping_in_queue = multiprocessing.Queue() #dough_out
        oven_in_queue = multiprocessing.Queue() #toppings out
        waiters_in_queue = multiprocessing.Queue() #oven out
        self.orders_out_queue = multiprocessing.Queue() #waiters out

        self.dough_to_toppings_pipe = DoughPipe(self.in_orders_queue, topping_in_queue,
                                                RestaurantConsts.DOUGH_CHEFS_NUM, RestaurantConsts.DOUGH_DELAY)
        self.toppings_to_oven_pipe = ToppingsPipe(topping_in_queue, oven_in_queue, RestaurantConsts.TOPPINGS_CHEFS_NUM,
                                                  RestaurantConsts.TOPPING_DELAY, RestaurantConsts.TOPPINGS_PER_WORKER)
        self.oven_to_waiters_pipe = OvenPipe(oven_in_queue, waiters_in_queue, RestaurantConsts.OVENS_NUM,
                                             RestaurantConsts.OVEN_DELAY)
        self.waiters_pipe = WaitersPipe(waiters_in_queue, self.orders_out_queue, RestaurantConsts.WAITERS_NUM,
                                        RestaurantConsts.DOUGH_DELAY)

    def stop_manage(self):
        tot = len(self.orders)
        while True:
            for name, _ in iter(self.orders_out_queue.get, None):
                for ind, order in enumerate(self.orders):
                    if order.name == name:
                        order.set_end_time(datetime.now())
                        tot -= 1
                if tot == 0:
                    self.stop_pipes()
                    self.update_orders_at_report()
                    return



    def update_orders_at_report(self):
        self.report_file = os.path.join('reports', 'report_' + time.strftime("%H_%M_%S", time.localtime()) + '.csv')

        with open(self.report_file, mode='w') as csv_file:
            fieldnames = [RestaurantConsts.ORDER_NAME, RestaurantConsts.ORDER_TOPPINGS, RestaurantConsts.START_TIME, RestaurantConsts.END_TIME, RestaurantConsts.TOT_TIME]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            self.end_time = datetime.now()
            writer.writerow({RestaurantConsts.ORDER_NAME: 'manage all orders', RestaurantConsts.ORDER_TOPPINGS: "None", RestaurantConsts.START_TIME: self.start_time,
                             RestaurantConsts.END_TIME: self.end_time,
                             RestaurantConsts.TOT_TIME: (self.end_time - self.start_time)})
            for order in self.orders:
                writer.writerow({RestaurantConsts.ORDER_NAME: order.name, RestaurantConsts.ORDER_TOPPINGS: order.toppings, RestaurantConsts.START_TIME: order.start_time,
                                 RestaurantConsts.END_TIME: order.end_time,
                                 RestaurantConsts.TOT_TIME: (order.end_time - order.start_time)})

        csv_file.close()

    def stop_pipes(self):
        self.dough_to_toppings_pipe.stop()
        self.toppings_to_oven_pipe.stop()
        self.oven_to_waiters_pipe.stop()
        self.waiters_pipe.stop()

    def print_report(self):
        df = pandas.read_csv(self.report_file, index_col=RestaurantConsts.ORDER_NAME)
        print(df)





if __name__ == '__main__':

    tests = [
        # [Order('cohen', []), Order('levi', []), Order('dan', []), Order('gil', [])],
        # [Order('cohen', [Toppings.BlackOlives]), Order('levi', [Toppings.Sausage, Toppings.ExtraCheese, Toppings.BlackOlives])],
        # [Order('cohen', [Toppings.BlackOlives, Toppings.Onion]), Order('levi', [Toppings.Sausage, Toppings.ExtraCheese])],
        # [Order('cohen', [Toppings.BlackOlives, Toppings.Onion]), Order('levi', [Toppings.Sausage, Toppings.ExtraCheese, Toppings.BlackOlives])],
        # [Order('cohen', [Toppings.BlackOlives]), Order('levi', [])],
        [Order('cohen', [Toppings.BlackOlives, Toppings.Sausage]), Order('levi', [])]
        ]

    #NOTE - there is a great shift at start time of orders because start time = initilze time
    for t in tests:
        manage_restaurant = ManageRestaurant(t)
        manage_restaurant.start_manage()
        manage_restaurant.stop_manage()

        manage_restaurant.print_report()

