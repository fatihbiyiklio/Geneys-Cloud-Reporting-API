#!/usr/bin/env python3
"""
Tüm Agentlardan Tüm Kuyrukları Sıfırlama Scripti
================================================
Bu script, Genesys Cloud'daki tüm kullanıcıları tüm kuyruklardan çıkarır.

Kullanım:
    python reset_all_queues.py

DİKKAT: Bu işlem geri alınamaz! Tüm agentlar tüm kuyruklardan çıkarılacaktır.
"""

import os
import sys
import json
import time
import getpass
import requests
from cryptography.fernet import Fernet

# ─────────────────────────────────────────────────────────────────────────────
# Yapılandırma
# ─────────────────────────────────────────────────────────────────────────────
ORG_CODE = "default"  # Organizasyon kodu (orgs klasöründeki klasör adı)
REGION = "mypurecloud.ie"  # Genesys Cloud bölgesi
DRY_RUN = False  # True = sadece simülasyon (değişiklik yapmaz), False = gerçek işlem

# ─────────────────────────────────────────────────────────────────────────────
# Yardımcı Fonksiyonlar
# ─────────────────────────────────────────────────────────────────────────────

def load_credentials(org_code):
    """Şifreli credentials dosyasından client_id ve client_secret yükler."""
    creds_path = os.path.join("orgs", org_code, "credentials.enc")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials dosyası bulunamadı: {creds_path}")
        return None, None
    
    # Şifre çözme anahtarı iste
    key = getpass.getpass("🔐 Credentials şifreleme anahtarını girin: ")
    
    try:
        fernet = Fernet(key.encode())
        with open(creds_path, "rb") as f:
            encrypted_data = f.read()
        decrypted_data = fernet.decrypt(encrypted_data).decode()
        creds = json.loads(decrypted_data)
        return creds.get("client_id"), creds.get("client_secret")
    except Exception as e:
        print(f"❌ Credentials çözülemedi: {e}")
        return None, None


def authenticate(client_id, client_secret, region):
    """Genesys Cloud'a bağlan ve access token al."""
    login_host = f"https://login.{region}"
    token_url = f"{login_host}/oauth/token"
    
    try:
        response = requests.post(
            token_url,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=10
        )
        
        if response.status_code == 200:
            token_data = response.json()
            return {
                "access_token": token_data["access_token"],
                "api_host": f"https://api.{region}"
            }
        else:
            print(f"❌ Auth hatası ({response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"❌ Bağlantı hatası: {e}")
        return None


def api_get(auth, path, params=None):
    """GET isteği gönder."""
    headers = {
        "Authorization": f"Bearer {auth['access_token']}",
        "Content-Type": "application/json"
    }
    response = requests.get(f"{auth['api_host']}{path}", headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def api_post(auth, path, data=None, params=None):
    """POST isteği gönder."""
    headers = {
        "Authorization": f"Bearer {auth['access_token']}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        f"{auth['api_host']}{path}",
        headers=headers,
        json=data,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response


def get_all_queues(auth):
    """Tüm kuyrukları çek."""
    queues = []
    page_number = 1
    while True:
        data = api_get(auth, "/api/v2/routing/queues", params={"pageNumber": page_number, "pageSize": 100})
        if "entities" in data:
            queues.extend(data["entities"])
            if not data.get("nextUri"):
                break
            page_number += 1
        else:
            break
    return queues


def get_queue_members(auth, queue_id):
    """Bir kuyruğun tüm üyelerini çek."""
    members = []
    page_number = 1
    while True:
        try:
            data = api_get(
                auth,
                f"/api/v2/routing/queues/{queue_id}/members",
                params={"pageNumber": page_number, "pageSize": 100, "member_by": "user"}
            )
            if "entities" in data:
                members.extend(data["entities"])
                if not data.get("nextUri"):
                    break
                page_number += 1
            else:
                break
        except Exception as e:
            print(f"  ⚠️ Üye listesi alınamadı: {e}")
            break
    return members


def remove_members_from_queue(auth, queue_id, member_ids, dry_run=True):
    """Bir kuyruktan üyeleri çıkar (100'lük gruplar halinde)."""
    if not member_ids:
        return 0, 0
    
    success_count = 0
    fail_count = 0
    
    # 100'lük gruplar halinde işle
    batch_size = 100
    for i in range(0, len(member_ids), batch_size):
        batch = member_ids[i:i + batch_size]
        body = [{"id": mid} for mid in batch]
        
        if dry_run:
            success_count += len(batch)
            print(f"    [DRY RUN] {len(batch)} üye çıkarılacak")
        else:
            try:
                api_post(auth, f"/api/v2/routing/queues/{queue_id}/members", data=body, params={"delete": "true"})
                success_count += len(batch)
            except Exception as e:
                fail_count += len(batch)
                print(f"    ❌ Hata: {e}")
        
        # Rate limit için bekleme
        time.sleep(0.2)
    
    return success_count, fail_count


# ─────────────────────────────────────────────────────────────────────────────
# Ana İşlem
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("🔄 TÜM AGENTLARDAN TÜM KUYRUKLARI SIFIRLAMA")
    print("=" * 60)
    
    if DRY_RUN:
        print("\n⚠️  DRY RUN MODU: Değişiklik yapılmayacak, sadece simülasyon.\n")
    else:
        print("\n🚨 GERÇEK MOD: Değişiklikler uygulanacak!\n")
        confirm = input("Devam etmek istiyor musunuz? (evet/hayır): ")
        if confirm.lower() not in ["evet", "e", "yes", "y"]:
            print("İşlem iptal edildi.")
            return
    
    # 1. Credentials yükle
    print(f"\n📁 Organizasyon: {ORG_CODE}")
    client_id, client_secret = load_credentials(ORG_CODE)
    if not client_id or not client_secret:
        # Manuel giriş seçeneği
        print("\n📝 Credentials'ı manuel girin:")
        client_id = input("Client ID: ").strip()
        client_secret = getpass.getpass("Client Secret: ").strip()
        if not client_id or not client_secret:
            print("❌ Credentials gerekli!")
            return
    
    # 2. Authenticate
    print(f"\n🔑 {REGION} bölgesine bağlanılıyor...")
    auth = authenticate(client_id, client_secret, REGION)
    if not auth:
        return
    print("✅ Bağlantı başarılı!")
    
    # 3. Tüm kuyrukları çek
    print("\n📋 Kuyruklar yükleniyor...")
    queues = get_all_queues(auth)
    print(f"   Toplam {len(queues)} kuyruk bulundu.")
    
    # 4. Her kuyruk için üyeleri çek ve çıkar
    total_removed = 0
    total_failed = 0
    
    for idx, queue in enumerate(queues, 1):
        queue_id = queue["id"]
        queue_name = queue["name"]
        
        print(f"\n[{idx}/{len(queues)}] 📦 {queue_name}")
        
        # Üyeleri çek
        members = get_queue_members(auth, queue_id)
        member_ids = [m.get("id") for m in members if m.get("id")]
        
        if not member_ids:
            print("   ✓ Üye yok, atlanıyor.")
            continue
        
        print(f"   {len(member_ids)} üye bulundu, çıkarılıyor...")
        
        success, failed = remove_members_from_queue(auth, queue_id, member_ids, dry_run=DRY_RUN)
        total_removed += success
        total_failed += failed
        
        if not DRY_RUN:
            print(f"   ✅ {success} üye çıkarıldı" + (f", ❌ {failed} başarısız" if failed else ""))
        
        # Rate limit
        time.sleep(0.3)
    
    # 5. Özet
    print("\n" + "=" * 60)
    print("📊 ÖZET")
    print("=" * 60)
    print(f"   Toplam kuyruk: {len(queues)}")
    print(f"   Çıkarılan üye: {total_removed}")
    if total_failed:
        print(f"   Başarısız: {total_failed}")
    
    if DRY_RUN:
        print("\n⚠️  Bu bir DRY RUN idi. Gerçek işlem için DRY_RUN = False yapın.")
    else:
        print("\n✅ İşlem tamamlandı!")


if __name__ == "__main__":
    main()
