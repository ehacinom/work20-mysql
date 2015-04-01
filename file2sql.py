

# file utilities and databases
import MySQLdb # Server version: 5.5.40 MySQL Community Server (GPL)
import os, sys, glob
import csv, gzip

# hidden exception classes
import _mysql_exceptions, _csv

# plotting
import numpy as np
import matplotlib
#matplotlib.use('GTKCairo') # forwarding to remote machine
import matplotlib.pyplot as plt

matplotlib.rc('font', weight='light') 
#matplotlib.rcParams.update({'font.size': 10})


from tabulate import tabulate


class file2sql:
    '''
    Move files in a directory to a database table. Plot relevant information.
    Assumed csv files and 1 header line. See README.md for other assumptions.


    USAGE

    credentials = ($HOST, $USER, $PASSWD, $DB)
    directory   = $DIR      # data directory
    name        = $NAME     # table name
    verbose     = False

    x = mySQLfile(credentials, directory, name)
    x.load_data()
    x.index_data()
    x.trends()


    GENERALIZE
    - index_data()
    - *_trends() and trends() functions

    '''

    def __init__(self, credentials, directory, name, verbose=True):
        '''Connect to database, find files to upload.'''
        self.credentials = credentials

        # check database exists, if not, create it, and connect
        connected = False
        db = credentials[3]
        while not connected:
            try:
                self.db = MySQLdb.connect(host   = credentials[0], 
                                          user   = credentials[1], 
                                          passwd = credentials[2], 
                                          db     = db)
                connected = True
            except _mysql_exceptions.OperationalError as e:
                print 'Database %s does not exist.' % db
                question = ('Create %s (0) or choose a different '
                            'database (1)? ' % db)
                selfdb = self.askthrice(question)
                if selfdb:
                    db = raw_input('Enter a new database name: ').strip()
                else:
                    tmpcon = MySQLdb.connect(host   = credentials[0], 
                                             user   = credentials[1], 
                                             passwd = credentials[2])
                    tmpcur = tmpcon.cursor()
                    cmd = 'CREATE DATABASE %s' % db
                    tmpcur.execute(cmd)
                    tmpcon.commit()
                    tmpcur.close()
        self.dbname = db
        self.cur = self.db.cursor()

        # checking the dir exists
        self.directory = directory
        if not os.path.isdir(directory):
            print '%s is not a directory.' % directory
            sys.exit()

        # checking the dir for sql injection?
        # checking the database for sql injection?

        # checking the name for sql injection
        self.name = self.scrub(str(name))
        if self.name != name:
            print 'Warning!'
            print 'name = %s is not alphanumeric and has been replaced by %s' \
                   % (str(name), self.name)

        # verbosity
        self.verbose = verbose

    def scrub(self, stmt):
        '''Scrub punctuation and whitespace from a string.'''
        return ''.join(c for c in stmt if c.isalnum() or c in '_-$')

    def askthrice(self, question):
        '''Ask a binary question three times, default 0'''
        ask, exitval = 0, 3
        while ask < exitval:
            ans = raw_input(question).strip()
            if ans == '1' or ans == '0':
                ask = exitval
            else: 
                ans = '0'
                ask += 1
        return int(ans)

    def get_files(self, pattern='*.csv*'):
        '''Get all files like pattern from directory and its subdirectories.'''
        files = []
        for d, _, _ in os.walk(self.directory):
            files.extend(glob.glob(os.path.join(d, pattern)))
        self.files = files

    def get_table(self):
        '''Check for preexisting table with the same name.

        0: load data
        1: (delete table), create table, load data
        '''
        check = 1
        cmd = "SHOW TABLES LIKE '%s'" % self.name
        self.cur.execute(cmd)
        if self.cur.fetchone():
            print 'You have a pre-existing table of the name \'%s\'.' % self.name
            question = 'Update table (0) or overwrite (1)? '
            check = self.askthrice(question)

            if check:
                # rm existing table
                destroyer = 'DROP TABLE IF EXISTS %s' % self.name
                self.cur.execute(destroyer)

                self.create_table()
        else:
            print 'There is no %s.%s table.' % (self.dbname, self.name)
            self.create_table()

    def create_table(self):
        '''Create a table.'''

        # creating a table requires knowing the header
        question = ('Will you use the default table creator (0) '
                    'or supply your own (1)? ')
        selftable = self.askthrice(question)
        if selftable: creator = self.input_table_create()
        else: creator = self.auto_table_create()

        # execute create, defaults coded in 
        try: 
            self.cur.execute(creator)
        except _mysql_exceptions.ProgrammingError as e:
            if selftable:
                err1 = ('Your creator command failed. It was:\n\t%s\n'
                        'The failure is probably due to MySQL reserved '
                        'words w/out backticks. Retrying using the '
                        'default table creator.' % repr(creator))
                print err1
                creator = self.auto_table_create()
                try:
                    self.cur.execute(creator)
                except _mysql_exceptions.ProgrammingError as e:
                    err0 = ('The default creator command failed.\n'
                            'Check the source code or your command for '
                            'column names that are RESERVED WORDS for the '
                            'MySQL server.\nThe command used was:\n\t%s' 
                            % creator)
                    print e
                    sys.exit(err0)
            else:
                err0 = ('The default creator command failed.\n'
                        'Check the source code or your command for column '
                        'names that are RESERVED WORDS for the MySQL '
                        'server.\nThe command used was:\n\t%s' % creator)
                print e
                sys.exit(err0)

    def auto_table_create(self):
        '''SQL command to create table from csv files.

        Create a string SQL command to set up the `creator` command
        used in load_data()

        All column types are strings :p
        '''
        # open a file to retrieve header
        r = csv.reader(open(self.files[0], 'rb'))
        try:
            header = r.next()
        except _csv.Error as e:
            print e
            sys.exit('Error: probably compressed files, not CSV files.')
        self.n = len(header)

        # check header is valid
        valid_columns = 0
        for column in header:
            num_chars = sum(c.isalpha() for c in column)
            if num_chars > 0:
                valid_columns += 1
        if valid_columns == self.n:
            # backticks for reserved words, see below
            self.cols = ['`'+c+'`' for c in header]
        else:
            self.cols = ['C'+str(i) for i in xrange(self.n)]

        # I dislike the problem of reserved words (mysql -v 5.5.40)
        # Should use sets to check for matches and remove, but I'm just
        # going to assume backticks and omniscient mysql database users

        # okay, iteratively create this SQL command `creator`
        middle = ' TEXT, '.join(self.cols)
        creator = 'CREATE TABLE %s (%s TEXT)' % (self.name, middle)

        return creator

    def input_table_create(self):
        '''Type in your own table-creator'''
        stmt = 'Type your SQL statement to create a %s.%s table here: ' \
                % (self.dbname, self.name)
        return raw_input(stmt)

    def load_data(self):
        '''CSV files to database.

        Three commands of note:
        destroyer: removes existing table
        creator:   creates table
        loader:    loads data into table
        '''

        # get files
        self.get_files(pattern='*.csv*')

        # init table
        self.get_table()

        for f in self.files:
            # Load data command
            loader = ("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS "
                      "TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES "
                      "TERMINATED BY '\r\n' IGNORE 1 LINES" 
                      % (f, self.name))
            self.cur.execute(loader)

            if self.verbose:
                print 'Importing %s into the %s table.' \
                       % (f, self.credentials[3] + '.' + self.name)

        # commit
        self.db.commit()
        #self.cur.close()

    def index_data(self):
        '''Index the database table.

        Not generalized.
        '''

        print 'Indexing table.'

        cols = ['query_date', 'Title', 'grade', 'review', 'appid']
        for col in cols:
            cmd = ("CREATE INDEX %s ON %s (%s)" % (col+'x', self.name, col))
            self.cur.execute(cmd)

        # commit
        self.db.commit()

    def time_trends(self):
        '''Plot daily trends.

        Not generalized.

        For each day:
            Total library full_price
            Total library discount_price
            Total n_reviews
        '''

        if self.verbose: print 'Retrieving time trends from database.'

        # CAREFUL
        # lists in columns of sql databases are bad.
        # ['Third-party', 'Install', 'Install Now'] are in DOUBLE discount_price
        cmd = ("SELECT query_date, sum(full_price)/1000, "
               "sum(discount_price)/1000, sum(n_reviews)/1000000. FROM steam " 
               + self.condition + "GROUP BY query_date")
        self.cur.execute(cmd)
        dates, full_price, discount_price, n_reviews = zip(*self.cur.fetchall())

        # plotting
        if self.verbose: print 'Plotting time trends.'
        g0, g1, g2 = '#3BCF3B', '#059305', '#98EC98'
        orange = '#FF9B49'
        fig, ax1 = plt.subplots()

        # prices in ax1
        line1 = ax1.plot(dates, full_price, color=g2, label = 'full price')
        line2 = ax1.plot(dates, discount_price, color=g1, label = 'discount price')

        # x-axis
        ax1.set_xlabel('date')
        ax1.set_ylabel('reviews (millions)')
        fig.autofmt_xdate() # rotate date axis automatically

        # y-axis in ax1
        ax1.set_ylabel('price ($1000)', color=g0)
        for tl in ax1.get_yticklabels():
            tl.set_color(g0)

        # reviews in ax2
        ax2 = ax1.twinx()
        line3 = ax2.plot(dates, n_reviews, color=orange, label = 'reviews')

        # y-axis in ax2
        ax2.set_ylabel('reviews (millions)', color=orange)
        for tl in ax2.get_yticklabels():
            tl.set_color(orange)

        # legend
        lines = line1 + line2 + line3
        label = [line.get_label() for line in lines]
        plt.legend(lines, label, loc = 'lower right')
        plt.title('Prices and reviews of the entire Steam games library')
        plt.savefig('time_trends.pdf')

    def game_trends(self):
        '''Tabulate game-specific trends.

        Not generalized. Scrub cmd if it is.

        For each game:
            Title, Min full_price, date
            Title, Max full_price, date
            Title, Min discount_price, date
            Title, Max discount_price, date
        '''

        # save each row of data as dictionary data[Title] = [Title, etc...]
        data = {}

        if self.verbose: print 'Retrieving game trends from database.'
        for price in ['full_price', 'discount_price']:
            for math in ['min', 'max']:
                # retrieve from db
                cmd = ("SELECT Title, " + math + "(" + price + 
                        "), query_date FROM steam " + self.condition + 
                        "GROUP BY Title")
                self.cur.execute(cmd)
                rows = self.cur.fetchall()

                # ASSUMPTION!
                # keys are all the same, no new keys will be found.
                # this is not that bad an assuption, as we're finding 
                # min and max, so that should be the same titles pulled out.
                # sure fix: in else statement, check length of data[r[0]]
                colname = math + ' ' + price
                if not data:
                    # add UTF8 decoded titles as keys
                    for r in rows: data[r[0]] = [r[0].decode('utf-8'), r[1], r[2]]
                    cols = ['title', colname, 'date']
                else:
                    for r in rows: data[r[0]].extend([r[1], r[2]])
                    cols.extend([colname, 'date'])

        # flatten
        data = [(k, v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8]) 
                for k,v in data.items()]

        # output
        if self.verbose: 
            print 'Tables of game trends.'
            print tabulate(data, cols)

        # save
        with open("game_trends.csv", "wb") as f:
            writer = csv.writer(f)
            writer.writerows(data)


    def review_trends(self):
        '''Review statistics

        Not generalized.

        For each review:
            
        '''
        pass

    def trends(self):
        '''Get trends of our specific table.

        Not generalized.
        '''

        self.condition = ("WHERE appid NOT LIKE '%,%' "
                          "AND discount_price LIKE '%.%' ")
        self.time_trends()
        #self.game_trends()
        #self.review_trends()