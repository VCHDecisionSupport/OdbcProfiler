from django.db import models

# Create your models here.
from django.db import models

# Create your models here.

class server_info(models.Model):
    # server_info = models.AutoField(primary_key=True)
    server_name = models.CharField(max_length=100)
    server_type = models.CharField(max_length=100)

class database_info(models.Model):
    # database_info = models.AutoField(primary_key=True)
    database_name = models.CharField(max_length=100)
    server_info = models.ForeignKey(server_info, default=0)

class view_table_info(models.Model):
    # view_table_info = models.AutoField(primary_key=True)
    ansi_view_table_name = models.CharField(max_length=100)
    pretty_view_table_name = models.CharField(max_length=100)
    database_info = models.ForeignKey(database_info, default=0)

class view_table_profile(models.Model):
    # view_table_profile = models.AutoField(primary_key=True)
    profile_date = models.DateTimeField('date this record was created')
    view_table_row_count = models.IntegerField()
    i_finally_got_it_working = models.IntegerField(default=123)
    view_table_info = models.ForeignKey(view_table_info, default=0)
    
