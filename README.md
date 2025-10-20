# DOF.not

DEMO: https://dofnot.chfotofilm.dk

DOF.not indsamler, filtrerer og viser danske fugleobservationer (fra DOFbasen) som “tråde” (Art × Lokation) i en enkel web‑UI. Systemet kan også sende webpush‑beskeder til klienter.

- birdnotification.py henter DOFbasens CSV pr. dag, finder første observation pr. (Artnavn × Loknr) i dansk tid, filtrerer pr. klientprofil og skriver batches til web/ og/eller sender webpush.
- server/ (FastAPI) serverer web‑UI’et fra web/, stiller API’er til rådighed og håndterer webpush‑abonnementer.
- Web‑UI (thread.html + thread.js) viser tråd‑oversigter og detaljer. I tidsrummet 00:00–03:00 (Europe/Copenhagen) kombineres “i dag” og “i gårs” index.json; efter 03:00 bruges kun i dag.

## Funktioner (kort)
- Første‑observation pr. Art × Loknr pr. dag (DK‑tid).
- Kategorier: alm | sub | su | bemærk (pr. region).
- Klient‑filtrering (arter, adfærd, lokation, tid, bbox, antal, kategori).
- Web‑UI med trådsammendrag, sortering og tråddetaljer.
- Valgfri webpush/digest (VAPID).

---

## Krav
- Windows med Python 3.11+ anbefales.
- Ingen Node krævet.

Eksterne Python‑pakker (se requirements.txt):
- fastapi, uvicorn[standard], pywebpush, http-ece
- requests, pandas, pyyaml, tzdata

## Installation

1) Installer requirements:
```
pip install -r requirements.txt
```

Valgfrit: tzdata sikrer tidszoner på Windows.

## Kørsel (to terminaler)

Åbn to terminaler i projektmappen.

Terminal A: start API/server (serverer web/ og API’er)
```
uvicorn server:app --reload --host 0.0.0.0 --port 8000 --log-level info --access-log
```

Terminal B: start indsamler/dispatcher (kontinuerlig polling)
```
python birdnotification.py --config clients.yaml --watch -i 60
```

Åbn web‑UI:
- Forside: http://localhost:8000/thread.html
- Tråddetalje (eksempel): http://localhost:8000/thread.html?date=YYYY-MM-DD&id=<thread_id>

Note om dansk tid:
- Web‑UI anvender Europe/Copenhagen. Kl. 00:00–03:00 kombineres `web/obs/<i dag>/index.json` og `web/obs/<i går>/index.json`; efter 03:00 bruges kun i dag.


## Konfiguration (kort)
- clients.yaml definerer klientprofiler og filtrering (arter, regioner, kategorier m.m.). Angives med --config.
- Webpush/VAPID: Angiv VAPID‑nøgler i serverens konfiguration/miljø (se server.py). Uden nøgler sendes der ikke push.

## Datastruktur
- web/obs/YYYY-MM-DD/index.json: tråd‑indeks for dagen.
- web/obs/YYYY-MM-DD/threads/: detaljer pr. tråd (hvis genereret).
- server/subscriptions.db: lokalt SQLite‑lager for webpush‑abonnementer.
- Evt. web/meta.json og web/feed.jsonl hvis output er slået til i indsamleren.

## Typisk workflow
1) Kør serveren (Uvicorn) for at hoste UI + API.
2) Kør birdnotification.py i watch‑mode for at hente/analysere CSV periodisk.
3) Åbn thread.html for at se tråde. Brug filtre og sortering i toppen.
4) Klik på en tråd for at se detaljerne, inkl. noter og evt. billeder.

# Docker (valgfrit)

Denne repo kan køres i Docker via docker-compose. Entrypoint-scriptet `entrypoint.sh` kører begge processer i samme container:
- Uvicorn (FastAPI-serveren)
- birdnotification.py (watcher)

Det er kun relevant ved Docker-kørsel. Ved lokal udvikling følg i stedet “Kørsel (to terminaler)”.

Miljøvariabler (kan sættes i .env):
- TZ, APP_VERSION
- UVICORN_HOST, UVICORN_PORT, UVICORN_LOG_LEVEL
- BIRD_CONF, BIRD_INTERVAL, BIRD_EXTRA_ARGS
- VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_CONTACT

Kør:

```
git clone https://github.com/thyholm1234/DOF.not.git
cd DOF.not
docker compose up -d --build
```

Åbn http://host_ip:8000/thread.html

Bemærk:
- `entrypoint.sh` kører inde i Linux-containeren (Bash). Det skal være eksekverbart (chmod +x), men Dockerfile håndterer det.

## Fejlfinding
- Port i brug: skift --port i uvicorn.
- CORS/HTTPS: se server.py for CORS‑opsætning.
- 404 på billeder: Sørg for at /api/obs/images?obsid=... er implementeret, eller tilpas klientens fetch.
- “Ingen tråde” 00–03: tjek at både web/obs/<i dag>/index.json og web/obs/<i går>/index.json findes.
- Tidszone: systemtid eller tzdata kan påvirke udregning af “i går”.

## Projektstruktur (forenklet)
```
DOF.not/
├─ web/
│  ├─ thread.html
│  ├─ thread.js
│  ├─ app.js
│  └─ obs/YYYY-MM-DD/index.json
├─ server/
│  └─ server.py
├─ birdnotification.py
├─ requirements.txt
└─ README.md
```

## Licens
MIT-licens

Ophavsret (c) 2025 DOF.not - Christian Vemmelund Helligsø

Hermed gives enhver, der opnår en kopi af denne software og tilhørende dokumentationsfiler (“Softwaren”), tilladelse uden vederlag til uden begrænsning at bruge Softwaren, herunder uden begrænsning rettighederne til at anvende, kopiere, modificere, sammenflette, publicere, distribuere, underlicensere og/eller sælge kopier af Softwaren, og til at tillade personer, som Softwaren leveres til, at gøre det samme, under følgende betingelser:

Ovenstående ophavsretsmeddelelse og denne tilladelsesmeddelelse skal medtages i alle kopier eller væsentlige dele af Softwaren.

SOFTWAREN LEVERES “SOM DEN ER”, UDEN NOGEN FORM FOR GARANTI, HVERKEN UDTRYKKELIG ELLER STILTIENDE, HERUNDER MEN IKKE BEGRÆNSET TIL GARANTIER OM SALGBARHED, EGNETHED TIL ET BESTEMT FORMÅL OG IKKE-KRÆNKELSE. UNDER INGEN OMSTÆNDIGHEDER SKAL FORFATTERNE ELLER OPHAVSRETSEJERNE VÆRE ANSVARLIGE FOR ETHVERT KRAV, SKADE ELLER ANDEN FORPLIGTELSE, HVERKEN I KONTRAKT, ERSTATNINGSRET ELLER PÅ ANDEN MÅDE, SOM OPSTÅR SOM FØLGE AF, UDE AF ELLER I FORBINDELSE MED SOFTWAREN ELLER BRUGEN AF ELLER ANDRE FORHOLD I SOFTWAREN.