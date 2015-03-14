Move files in a directory to a database table. Plot relevant information.

Usage
=====

credentials = ($HOST, $USER, $PASSWD, $DB)
directory   = $DIR      # data directory
name        = $NAME     # table name
verbose     = False

x = mySQLfile(credentials, directory, name)
x.get_files()
x.load_data()
x.index_data()
x.trends()

Relevant information
=======

`time_trends.pdf` shows the trends over time of the total library cost (full_price and discount_price in green) and the number of reviews (in purple). Note the heavily discounted prices near Thanksgiving and Christmas holiday seasons.

`game_trends.csv` is a table of [`title`, `min full_price`,  `date`, `max full_price`, `date`, `min discount_price`, `date`, `max discount_price`, `date`] for each game.



Notes
=====

Assumptions:
- csv files
- 1 header line
- several to do with the data we use having non-Null fields
- bundles do not exist

Generalize
- index_data(self)
- *_trends() and trends() functions

