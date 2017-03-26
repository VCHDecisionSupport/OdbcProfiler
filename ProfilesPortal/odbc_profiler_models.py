# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from __future__ import unicode_literals

from django.db import models


class ColumnHistogram(models.Model):
    column_histogram_id = models.AutoField(primary_key=True)
    column_profile_id = models.IntegerField()
    column_info_id = models.IntegerField()
    column_value_count = models.IntegerField(blank=True, null=True)
    column_value_string = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'column_histogram'


class ColumnInfo(models.Model):
    column_info_id = models.AutoField(primary_key=True)
    table_info = models.ForeignKey('TableInfo', models.DO_NOTHING)
    ansi_full_column_name = models.CharField(max_length=100, blank=True, null=True)
    ansi_full_table_name = models.CharField(max_length=100, blank=True, null=True)
    column_name = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'column_info'


class ColumnProfile(models.Model):
    column_profile_id = models.AutoField(primary_key=True)
    column_info_id = models.IntegerField()
    table_profile = models.ForeignKey('TableProfile', models.DO_NOTHING)
    column_distinct_count = models.IntegerField(blank=True, null=True)
    column_distinct_count_execution_seconds = models.DecimalField(max_digits=5, decimal_places=1, blank=True, null=True)
    column_distinct_count_datetime = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'column_profile'


class DatabaseInfo(models.Model):
    database_info_id = models.AutoField(primary_key=True)
    server_info = models.ForeignKey('ServerInfo', models.DO_NOTHING)
    database_name = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'database_info'


class ServerInfo(models.Model):
    server_info_id = models.AutoField(primary_key=True)
    server_name = models.CharField(max_length=100)
    server_type = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'server_info'
        unique_together = (('server_name', 'server_type'),)


class TableInfo(models.Model):
    table_info_id = models.AutoField(primary_key=True)
    database_info = models.ForeignKey(DatabaseInfo, models.DO_NOTHING)
    ansi_full_table_name = models.CharField(max_length=100)
    schema_name = models.CharField(max_length=100, blank=True, null=True)
    table_name = models.CharField(max_length=100, blank=True, null=True)
    logical_path = models.CharField(max_length=100, blank=True, null=True)
    table_type = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'table_info'


class TableProfile(models.Model):
    table_profile_id = models.AutoField(primary_key=True)
    table_info = models.ForeignKey(TableInfo, models.DO_NOTHING)
    table_row_count = models.IntegerField(blank=True, null=True)
    table_row_count_datetime = models.DateTimeField(blank=True, null=True)
    table_row_count_execution_seconds = models.DecimalField(max_digits=5, decimal_places=1, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'table_profile'
        unique_together = (('table_profile_id', 'table_row_count_datetime'),)
