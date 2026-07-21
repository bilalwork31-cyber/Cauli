from django.contrib import admin

from .models import BackfillJob, Campaign, Recipient, SendLog, WebhookInboxItem


@admin.register(Campaign)
class CampaignAdmin(admin.ModelAdmin):
    list_display = ("cid", "status", "total_recipients", "n_pages")


@admin.register(Recipient)
class RecipientAdmin(admin.ModelAdmin):
    list_display = ("rid", "campaign", "page_id", "status", "attempts", "sent_flag")
    list_filter = ("status",)


@admin.register(SendLog)
class SendLogAdmin(admin.ModelAdmin):
    list_display = ("recipient_rid", "campaign_cid", "message_id", "sent_at_ms")


@admin.register(WebhookInboxItem)
class WebhookInboxItemAdmin(admin.ModelAdmin):
    list_display = ("item_key", "status", "attempts", "next_due_ms")
    list_filter = ("status",)


@admin.register(BackfillJob)
class BackfillJobAdmin(admin.ModelAdmin):
    list_display = ("id", "worker", "pages_fetched", "errors", "finished")
