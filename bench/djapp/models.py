from django.db import models


class BenchIo(models.Model):
    """Maps onto the existing bench_io table (managed=False -- setup.sh
    already created it; every other PG lane in this suite writes to the
    same table via raw SQL, this one goes through Django's ORM).
    """

    id = models.BigAutoField(primary_key=True)
    payload = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = "djapp"
        db_table = "bench_io"
        managed = False
