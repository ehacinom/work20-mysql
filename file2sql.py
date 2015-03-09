# Server version: 5.5.40 MySQL Community Server (GPL)
import MySQLdb
import os, sys, glob
import csv

# http://sourceforge.net/p/mysql-python/discussion/70461/thread/21fb6268/
import _mysql_exceptions


class file2sql:
    '''
    Move files in a directory to a database table.

    Usage

    credentials = ($HOST, $USER, $PASSWD, $DB)
    directory   = $DIR    # data directory
    name        = $NAME   # table name

    x = mySQLfile(credentials, directory, name)
    x.csv2db()

    '''

    def __init__(self, credentials, directory, name):
        '''Connect to database, find files to upload.'''
        # connect
        self.credentials = credentials
        self.db = MySQLdb.connect(host   = self.credentials[0], 
                                  user   = self.credentials[1], 
                                  passwd = self.credentials[2], 
                                  db     = self.credentials[3])
        self.cur = self.db.cursor()

        # checking the dir exists
        self.directory = directory
        if not os.path.isdir(directory):
            print '%s is not a directory.' % directory
            sys.exit()

        # checking the dir for sql insertion?

        # checking the name for sql insertion
        self.name = self.scrub(str(name))
        if self.name != name:
            print 'Warning!'
            print 'name = %s is not alphanumeric and has been replaced by %s' \
                   % (str(name), self.name)

        # getting files
        self.get_files(pattern='*.csv*')

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

    def get_files(self, pattern):
        '''Get all files from directory and its subdirectories.'''
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
            
            # creating a table requires knowing the header
            question = ('Will you use the default table creator (0) '
                        'or supply your own (1)? ')
            selftable = self.askthrice(question)
            if selftable: creator = self.selftablecreator()
            else: creator = self.tablecreator()
            
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
                    creator = self.tablecreator()
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

    def tablecreator(self):
        '''SQL command to create table.

        Create a string SQL command to set up the `creator` command
        used in csv2db()

        All column types are strings :p
        '''
        r = csv.reader(open(self.files[0], 'rb'))
        header = r.next()
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

        # I hate the problem of reserved words (mysql -v 5.5.40)
        # Should use sets to check for matches and remove, but I'm just
        # going to assume backticks and omniscient mysql database users

        # okay, iteratively create this SQL command `creator`
        middle = ' TEXT, '.join(self.cols)
        creator = 'CREATE TABLE %s (%s TEXT)' % (self.name, middle)

        return creator

    def selftablecreator(self):
        '''Type in your own table-creator'''
        stmt = 'Type your SQL statement to create a %s.%s table from %s here: ' \
                % (self.credentials[3], self.name, self.csvfile)
        return raw_input(stmt)

    def csv2db(self):
        '''CSV files to database
        
        Three commands of note:
        destroyer: removes existing table
        creator:   creates table
        loader:    loads data into table
        '''

        # init table
        self.get_table()

        for f in self.files:
            # Load data command
            loader = ("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS "
                      "TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES "
                      "TERMINATED BY '\n' IGNORE 1 LINES" 
                      % (f, self.name))
            print 'Importing %s into the %s table.' \
                   % (f, self.credentials[3] + '.' + self.name)
            self.cur.execute(loader)
            
        # commit
        self.db.commit()
        self.cur.close()

        return 1

