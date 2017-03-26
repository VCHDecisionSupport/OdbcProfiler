from django.contrib import admin

# Register your models here.
from .models import ColumnHistogram,ColumnInfo,ColumnProfile,DatabaseInfo,ServerInfo,TableInfo,TableProfile,ServerInfoAdmin


admin.site.register(ColumnHistogram)
admin.site.register(ColumnInfo)
admin.site.register(ColumnProfile)
admin.site.register(DatabaseInfo)
admin.site.register(ServerInfo, ServerInfoAdmin)
admin.site.register(TableInfo)
admin.site.register(TableProfile)