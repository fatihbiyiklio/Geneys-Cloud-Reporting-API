#!/usr/bin/env python3
"""
Debug script: Semih ALTUNEL 13:27 Available→On Queue değişikliği için
hangi API endpoint'inde actor (Fatih Özkaynak) bilgisinin bulunduğunu keşfeder.
"""
import os
import sys
import json

# Proje kök dizini
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.auth import authenticate
from src.api import GenesysAPI

# — Yapılandırma —
REGION = "mypurecloud.ie"
SEMIH_USER_ID = "18598053-ee03-458a-8482-c3bad9e5207e"
TARGET_TIME = "2026-02-21T13:27:40"  # UTC+3 → UTC = 10:27:40
# UTC interval (±2 saat pencere)
INTERVAL = "2026-02-21T08:00:00.000Z/2026-02-21T15:00:00.000Z"


def main():
    # Credentials yükle
    creds_path = os.path.join("orgs", "default", "credentials.enc")
    if not os.path.exists(creds_path):
        print("❌ Credentials dosyası bulunamadı")
        return

    import getpass
    from cryptography.fernet import Fernet
    key = getpass.getpass("🔐 Credentials şifreleme anahtarını girin: ")
    try:
        fernet = Fernet(key.encode())
        with open(creds_path, "rb") as f:
            encrypted_data = f.read()
        decrypted_data = fernet.decrypt(encrypted_data).decode()
        creds = json.loads(decrypted_data)
        client_id = creds.get("client_id")
        client_secret = creds.get("client_secret")
    except Exception as e:
        print(f"❌ Credentials hatası: {e}")
        return

    # Authenticate
    print(f"\n🔑 {REGION} bölgesine bağlanılıyor...")
    auth_result = authenticate(client_id, client_secret, REGION)
    if not auth_result:
        print("❌ Bağlantı hatası")
        return
    print("✅ Bağlantı başarılı!")

    api = GenesysAPI(auth_result)

    # ═══════════════════════════════════════════════════════════════
    # TEST 1: Audit Realtime — serviceName=Presence, EntityId filtresi
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("TEST 1: Audit Realtime — Presence + EntityId")
    print("=" * 70)
    try:
        resp = api.query_audits_realtime(
            interval=INTERVAL,
            service_name="Presence",
            filters=[{"property": "EntityId", "value": SEMIH_USER_ID}],
            page_size=50,
            sort_order="ascending",
            expand_user=True,
        )
        entities = (resp or {}).get("entities") or []
        print(f"Toplam: {(resp or {}).get('total', '?')}, Sayfa: {len(entities)} event")
        for i, evt in enumerate(entities[:30]):
            user = evt.get("user") or {}
            entity = evt.get("entity") or {}
            changes = evt.get("propertyChanges") or evt.get("changes") or []
            print(f"\n  [{i}] eventDate: {evt.get('eventDate')}")
            print(f"      serviceName: {evt.get('serviceName')}")
            print(f"      action: {evt.get('action')}")
            print(f"      entity.id: {entity.get('id')}, entity.type: {entity.get('type')}")
            print(f"      user.id: {user.get('id')}, user.name: {user.get('name')}")
            if changes:
                for ch in changes[:3]:
                    print(f"      change: {ch}")
    except Exception as e:
        print(f"  ❌ Hata: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TEST 2: Audit Realtime — serviceName=Routing, EntityId filtresi
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("TEST 2: Audit Realtime — Routing + EntityId")
    print("=" * 70)
    try:
        resp = api.query_audits_realtime(
            interval=INTERVAL,
            service_name="Routing",
            filters=[{"property": "EntityId", "value": SEMIH_USER_ID}],
            page_size=50,
            sort_order="ascending",
            expand_user=True,
        )
        entities = (resp or {}).get("entities") or []
        print(f"Toplam: {(resp or {}).get('total', '?')}, Sayfa: {len(entities)} event")
        for i, evt in enumerate(entities[:20]):
            user = evt.get("user") or {}
            entity = evt.get("entity") or {}
            changes = evt.get("propertyChanges") or evt.get("changes") or []
            print(f"\n  [{i}] eventDate: {evt.get('eventDate')}")
            print(f"      serviceName: {evt.get('serviceName')}")
            print(f"      action: {evt.get('action')}")
            print(f"      entity.id: {entity.get('id')}, entity.type: {entity.get('type')}")
            print(f"      user.id: {user.get('id')}, user.name: {user.get('name')}")
            if changes:
                for ch in changes[:3]:
                    print(f"      change: {ch}")
    except Exception as e:
        print(f"  ❌ Hata: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TEST 3: Audit Realtime — FİLTRESİZ (tüm servisler), dar zaman
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("TEST 3: Audit Realtime — Filtresiz, dar zaman (10:20-10:35 UTC)")
    print("=" * 70)
    narrow_interval = "2026-02-21T10:20:00.000Z/2026-02-21T10:35:00.000Z"
    try:
        resp = api.query_audits_realtime(
            interval=narrow_interval,
            service_name=None,
            filters=None,
            page_size=100,
            sort_order="ascending",
            expand_user=True,
        )
        entities = (resp or {}).get("entities") or []
        print(f"Toplam: {(resp or {}).get('total', '?')}, Sayfa: {len(entities)} event")
        # Semih veya Fatih ile ilgili olanları filtrele
        semih_lower = SEMIH_USER_ID.lower()
        fatih_keywords = ["fatih", "özkaynak", "ozkaynak"]
        for i, evt in enumerate(entities):
            evt_str = json.dumps(evt, ensure_ascii=False).lower()
            is_related = semih_lower in evt_str or any(k in evt_str for k in fatih_keywords)
            if is_related:
                user = evt.get("user") or {}
                entity = evt.get("entity") or {}
                changes = evt.get("propertyChanges") or evt.get("changes") or []
                print(f"\n  [{i}] ★ eventDate: {evt.get('eventDate')}")
                print(f"      serviceName: {evt.get('serviceName')}")
                print(f"      action: {evt.get('action')}")
                print(f"      entity.id: {entity.get('id')}, entity.type: {entity.get('type')}")
                print(f"      entity.name: {entity.get('name')}")
                print(f"      user.id: {user.get('id')}, user.name: {user.get('name')}")
                if changes:
                    for ch in changes[:5]:
                        print(f"      change: {ch}")
                # Tüm event'i raw olarak da yazdır
                print(f"      RAW KEYS: {list(evt.keys())}")
    except Exception as e:
        print(f"  ❌ Hata: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TEST 4: Service Mapping — Hangi servisler var?
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("TEST 4: Service Mapping (realtime)")
    print("=" * 70)
    try:
        mapping = api.get_audit_query_service_mapping(realtime=True)
        if isinstance(mapping, dict):
            services = mapping.get("services") or mapping.get("serviceMappings") or []
            if isinstance(services, list):
                for svc in services:
                    if isinstance(svc, dict):
                        name = svc.get("name") or svc.get("serviceName") or ""
                        entities_list = svc.get("entities") or []
                        entity_names = [e.get("name") or "" for e in entities_list if isinstance(e, dict)]
                        print(f"  {name}: {entity_names[:5]}")
            elif isinstance(services, dict):
                for k, v in services.items():
                    print(f"  {k}: {v}")
            else:
                print(f"  Raw: {json.dumps(mapping, ensure_ascii=False)[:500]}")
        else:
            print(f"  Raw type: {type(mapping)}, value: {str(mapping)[:500]}")
    except Exception as e:
        print(f"  ❌ Hata: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TEST 5: Async Audit Query — serviceName=Presence (daha geniş tarih)
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("TEST 5: Async Audit — Presence + EntityId (geniş tarih)")
    print("=" * 70)
    try:
        resp = api.query_audits(
            interval=INTERVAL,
            service_name="Presence",
            filters=[{"property": "EntityId", "value": SEMIH_USER_ID}],
            page_size=50,
            max_pages=5,
            expand_user=True,
        )
        entities = (resp or {}).get("entities") or []
        print(f"Toplam: {len(entities)} event, state: {resp.get('state')}")
        for i, evt in enumerate(entities[:20]):
            user = evt.get("user") or {}
            entity = evt.get("entity") or {}
            changes = evt.get("propertyChanges") or evt.get("changes") or []
            print(f"\n  [{i}] eventDate: {evt.get('eventDate')}")
            print(f"      serviceName: {evt.get('serviceName')}")
            print(f"      action: {evt.get('action')}")
            print(f"      entity.id: {entity.get('id')}, entity.type: {entity.get('type')}")
            print(f"      user.id: {user.get('id')}, user.name: {user.get('name')}")
            if changes:
                for ch in changes[:3]:
                    print(f"      change: {ch}")
    except Exception as e:
        print(f"  ❌ Hata: {e}")

    print("\n✅ Tüm testler tamamlandı!")


if __name__ == "__main__":
    main()
