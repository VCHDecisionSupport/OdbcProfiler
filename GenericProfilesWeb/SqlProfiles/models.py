from django.db import models

# Create your models here.

class ServerInfo(models.Model):
    ServerInfoID = models.AutoField(primary_key=True)
    ServerName = models.CharField(max_length=100)


class DatabaseInfo(models.Model):
    DatabaseInfoID = models.AutoField(primary_key=True)
    DatabaseName = models.CharField(max_length=100)
    ServerInfoID = models.ForeignKey(ServerInfo)


class TableViewInfo(models.Model):
    TableViewInfoID = models.AutoField(primary_key=True)
    TableViewInfoName = models.CharField(max_length=100)
    DatabaseInfoID = models.ForeignKey(DatabaseInfo)


class TableViewProfile(models.Model):
    TableViewProfileID = models.AutoField(primary_key=True)
    TableViewProfileDate = models.DateTimeField('date this record was created')
    RowCount = models.IntegerField()
