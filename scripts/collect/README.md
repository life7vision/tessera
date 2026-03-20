# Sports Data Collection Scripts

Tessera için veri toplama scriptleri. Veriler `data/` altına indirilir,
ardından Forge arayüzü üzerinden Tessera'ya ingest edilir.

---

## football_bulk.py — football-data.co.uk

**Kaynak:** https://www.football-data.co.uk
**Lisans:** Kişisel / araştırma kullanımı ücretsiz. Ticari yeniden dağıtım yasak.
**Kapsam:** 38 lig, 1993/94'ten bugüne, 10 büyük bahisçi oranı

### Veri İçeriği

Her CSV dosyası şunları içerir:

| Alan | Açıklama |
|------|----------|
| Date, HomeTeam, AwayTeam | Maç bilgisi |
| FTHG, FTAG, FTR | Tam skor (ev/deplasman/sonuç) |
| HTHG, HTAG, HTR | İlk yarı skoru |
| HS, AS, HST, AST | Şut / İsabetli şut |
| HC, AC | Korner |
| HF, AF | Faul |
| HY, AY, HR, AR | Sarı/Kırmızı kart |
| B365H/D/A | Bet365 oranları (Ev/Beraberlik/Deplasman) |
| BWH/D/A | Bwin oranları |
| IWH/D/A | Interwetten oranları |
| PSH/D/A | Pinnacle oranları |
| WHH/D/A | William Hill oranları |
| VCH/D/A | VC Bet oranları |
| MaxH/D/A | Maksimum oran (tüm bahisçiler) |
| AvgH/D/A | Ortalama oran |

### Kurulum

```bash
# Proje kök dizininden:
.venv/bin/pip install requests tqdm  # zaten kurulu olmalı
```

### Kullanım

```bash
# Tüm ligler, tüm sezonlar (1993-2025):
.venv/bin/python scripts/collect/football_bulk.py

# Sadece Süper Lig ve Premier League, 2015'ten itibaren:
.venv/bin/python scripts/collect/football_bulk.py --leagues T1 E0 --from-year 2015

# Ne indireceğini göster, indirme:
.venv/bin/python scripts/collect/football_bulk.py --dry-run

# Sadece extra ligler (Arjantin, Brezilya vb.):
.venv/bin/python scripts/collect/football_bulk.py --extra-only

# Zaten indirilen dosyaları atla (varsayılan davranış):
.venv/bin/python scripts/collect/football_bulk.py

# Zorla yeniden indir:
.venv/bin/python scripts/collect/football_bulk.py --force
```

### Çıktı Yapısı

```
data/football_data/
├── main/
│   ├── 9394/          # 1993/94 sezonu
│   │   ├── E0.csv     # Premier League
│   │   ├── T1.csv     # Süper Lig
│   │   └── ...
│   ├── 9495/
│   └── ... (2526/)
└── extra/
    ├── ARG.csv        # Arjantin (tüm sezonlar)
    ├── BRA.csv
    └── ...
```

### Tahmini Boyutlar

| Kapsam | Dosya Sayısı | Tahmini Boyut |
|--------|-------------|---------------|
| Tüm main ligler tüm sezonlar | ~700 CSV | ~150 MB |
| Extra ligler | 16 CSV | ~20 MB |
| Toplam | ~716 CSV | ~170 MB |

### Tessera'ya Ingest

Topladıktan sonra Forge → İngest arayüzünden veya CLI ile:

```bash
tessera ingest --source football_data --ref "T1/2425"
tessera ingest --source football_data --ref "E0/2324"
```

---

## Planlanan: other_sports.py — API-Sports

Basketbol, tenis, voleybol için API-Sports connector'ı.
API key: `API_SPORTS_KEY` env var olarak `.env`'e eklenecek.

## Planlanan: odds_bulk.py — The Odds API

Çok bahisçi kapsamı için The Odds API.
API key: `ODDS_API_KEY` env var olarak `.env`'e eklenecek.
