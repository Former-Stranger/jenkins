#!/usr/bin/env python3
"""
Export concert database from Firestore to JSON format for static website
"""
import sys, json
from collections import defaultdict
from pathlib import Path
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore




def init_firebase():
    """Initialize Firebase Admin SDK"""
    try:
        firebase_admin.get_app()
    except ValueError:
        try:
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred)
        except Exception as e:
            print(f"Could not use application default credentials: {e}")
            print("\nTo fix this, run:")
            print("  gcloud auth application-default login")
            print("\nOr download a service account key and set GOOGLE_APPLICATION_CREDENTIALS:")
            print("  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json")
            sys.exit(1)
    return firestore.client()


def export_to_json(output_dir):
    """Export Firestore database to JSON files for web"""

    db = init_firebase()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Exporting Firestore database to JSON...")
    print("=" * 60)

    # 1. Export all concerts with basic info
    print("\n1. Exporting concerts...")
    concerts_ref = db.collection('concerts')
    concerts_docs = concerts_ref.order_by('date', direction=firestore.Query.DESCENDING).stream()

    concerts_list = []
    all_concerts_data = {}  # Store for later use

    for doc in concerts_docs:
        data = doc.to_dict()
        concert_id = doc.id
        all_concerts_data[concert_id] = data

        # Get primary artist names for display
        artists_list = data.get('artists', [])
        # Filter to headliners and festival performers only
        primary_artists = [a for a in artists_list if a.get('role') in ['headliner', 'festival_performer']]
        artist_names = ', '.join([a.get('artist_name', '') for a in primary_artists])

        concerts_list.append({
            'id': concert_id,
            'show_number': data.get('show_number'),
            'date': data.get('date', ''),
            'festival_name': data.get('festival_name'),
            'venue': data.get('venue_name', ''),
            'city': data.get('city', ''),
            'state': data.get('state', ''),
            'artists': artist_names,
            'hasSetlist': False,  # Will be updated later if setlists exist
            'setlist_status': data.get('setlist_status', 'not_researched')
        })

    # Don't write concerts.json yet - need to update hasSetlist flags first
    print(f"   Loaded {len(concerts_list)} concerts")

    # 2. Export concert details (one file per concert with setlist)
    print("\n2. Exporting concert details with setlists...")
    details_dir = output_dir / 'concert_details'
    details_dir.mkdir(exist_ok=True)

    # Get all setlists and group by concert_id
    setlists_ref = db.collection('setlists')
    setlists_docs = setlists_ref.stream()

    # Group setlists by concert_id
    setlists_by_concert = defaultdict(list)
    for setlist_doc in setlists_docs:
        setlist_data = setlist_doc.to_dict()
        concert_id = setlist_data.get('concert_id')

        if concert_id and concert_id in all_concerts_data:
            # Include all setlists, even if empty (to show supporting acts/multiple artists)
            setlists_by_concert[concert_id].append(setlist_data)

    concerts_with_setlists = 0
    for concert_id, setlist_list in setlists_by_concert.items():
        concert_data = all_concerts_data[concert_id]

        # Get artists for this concert
        artists = [{'name': a.get('artist_name', ''), 'role': a.get('role', '')}
                   for a in concert_data.get('artists', [])]

        # Get photos for this concert
        photos_query = db.collection('concert_photos').where('concert_id', '==', concert_id).order_by('uploaded_at', direction=firestore.Query.DESCENDING)
        photos_docs = photos_query.stream()

        photos = []
        for photo_doc in photos_docs:
            photo_data = photo_doc.to_dict()
            # Convert timestamp to ISO string if it exists
            uploaded_at = photo_data.get('uploaded_at')
            uploaded_at_str = uploaded_at.isoformat() if uploaded_at else None

            photos.append({
                'id': photo_doc.id,
                'user_name': photo_data.get('user_name', ''),
                'user_photo': photo_data.get('user_photo', ''),
                'download_url': photo_data.get('download_url', ''),
                'uploaded_at': uploaded_at_str,
                'caption': photo_data.get('caption', ''),
                'file_type': photo_data.get('file_type', '')
            })

        concert_detail = {
            'id': concert_id,
            'show_number': concert_data.get('show_number'),
            'date': concert_data.get('date', ''),
            'festival_name': concert_data.get('festival_name'),
            'venue': concert_data.get('venue_name', ''),
            'city': concert_data.get('city', ''),
            'state': concert_data.get('state', ''),
            'artists': artists,
            'photos': photos,
            'photo_count': len(photos)
        }

        # Handle single vs multiple setlists
        if len(setlist_list) == 1:
            # Single setlist (backward compatible format)
            setlist_data = setlist_list[0]
            formatted_songs = []
            for song in setlist_data.get('songs', []):
                song_obj = {
                    'position': song.get('position', 0),
                    'name': song.get('name', ''),
                    'set_name': song.get('set_name', ''),
                    'encore': song.get('encore', 0),
                    'is_cover': song.get('is_cover', False),
                    'cover_artist': song.get('cover_artist')
                }
                # Add guest artist if present
                if song.get('guest_artist'):
                    song_obj['guest_artist'] = song.get('guest_artist')
                formatted_songs.append(song_obj)

            concert_detail.update({
                'setlistfm_url': setlist_data.get('setlistfm_url'),
                'song_count': setlist_data.get('song_count', 0),
                'has_encore': setlist_data.get('has_encore', False),
                'songs': formatted_songs
            })

            # Add tour name if present
            if setlist_data.get('tour_name'):
                concert_detail['tour_name'] = setlist_data.get('tour_name')
        else:
            # Multiple setlists (co-headliners or with openers)
            formatted_setlists = []
            total_song_count = 0
            has_any_encore = False
            tour_names = set()

            # Sort setlists by artist role: openers first, then headliners
            # Create a mapping of artist_name to role from the concert data
            # Note: Use artist_name instead of artist_id because setlist artist_id (MusicBrainz)
            # doesn't match concert artist_id (Firestore doc ID)
            artist_roles = {a.get('artist_name'): a.get('role', 'headliner')
                          for a in concert_data.get('artists', [])}

            # Role priority for sorting (lower number = appears first)
            role_priority = {'opener': 1, 'headliner': 2, 'festival_performer': 2}

            # Sort setlists: openers first, then headliners, then by artist name
            sorted_setlists = sorted(setlist_list,
                                    key=lambda s: (role_priority.get(artist_roles.get(s.get('artist_name'), 'headliner'), 2),
                                                  s.get('artist_name', '')))

            for setlist_data in sorted_setlists:
                formatted_songs = []
                for song in setlist_data.get('songs', []):
                    song_obj = {
                        'position': song.get('position', 0),
                        'name': song.get('name', ''),
                        'set_name': song.get('set_name', ''),
                        'encore': song.get('encore', 0),
                        'is_cover': song.get('is_cover', False),
                        'cover_artist': song.get('cover_artist')
                    }
                    # Add guest artist if present
                    if song.get('guest_artist'):
                        song_obj['guest_artist'] = song.get('guest_artist')
                    formatted_songs.append(song_obj)

                setlist_obj = {
                    'artist_id': setlist_data.get('artist_id'),
                    'artist_name': setlist_data.get('artist_name'),
                    'artist_role': artist_roles.get(setlist_data.get('artist_name'), 'headliner'),
                    'setlistfm_url': setlist_data.get('setlistfm_url'),
                    'song_count': setlist_data.get('song_count', 0),
                    'has_encore': setlist_data.get('has_encore', False),
                    'songs': formatted_songs
                }

                # Add tour name to setlist if present
                if setlist_data.get('tour_name'):
                    setlist_obj['tour_name'] = setlist_data.get('tour_name')
                    tour_names.add(setlist_data.get('tour_name'))

                formatted_setlists.append(setlist_obj)

                total_song_count += setlist_data.get('song_count', 0)
                if setlist_data.get('has_encore', False):
                    has_any_encore = True

            concert_detail.update({
                'setlists': formatted_setlists,
                'total_song_count': total_song_count,
                'has_encore': has_any_encore
            })

            # Add tour name if all setlists have the same tour
            if len(tour_names) == 1:
                concert_detail['tour_name'] = list(tour_names)[0]

        with open(details_dir / f'{concert_id}.json', 'w') as f:
            json.dump(concert_detail, f, indent=2)

        concerts_with_setlists += 1

    print(f"   Exported {concerts_with_setlists} concert details")

    # Clean up stale detail files for concerts that no longer have setlists
    concert_ids_with_setlists_set = set(setlists_by_concert.keys())
    existing_detail_files = list(details_dir.glob('*.json'))
    deleted_count = 0
    for detail_file in existing_detail_files:
        concert_id = detail_file.stem  # filename without extension
        if concert_id not in concert_ids_with_setlists_set:
            detail_file.unlink()
            deleted_count += 1
            print(f"   Deleted stale detail file: {concert_id}.json")

    if deleted_count > 0:
        print(f"   Cleaned up {deleted_count} stale detail file(s)")

    # Now update hasSetlist flags for concerts that have setlists
    concert_ids_with_setlists = set(setlists_by_concert.keys())
    for concert in concerts_list:
        if concert['id'] in concert_ids_with_setlists:
            concert['hasSetlist'] = True

    # Write concerts.json with updated hasSetlist flags
    # Before (pylint warning)
    with open(output_dir / 'concerts.json', 'w', encoding='utf-8') as f:
        json.dump(concerts_list, f, indent=2)

    # 3. Export artists list
    print("\n3. Exporting artists...")
    artists_ref = db.collection('artists')
    artists_docs = artists_ref.stream()

    # Count concerts per artist from concert data
    artist_concert_counts = defaultdict(int)
    artist_names_map = {}

    for artist_doc in artists_docs:
        artist_data = artist_doc.to_dict()
        artist_id = artist_doc.id
        artist_names_map[artist_id] = artist_data.get('canonical_name', '')

    # Count concerts for each artist (includes headliners, openers, and festival performers)
    for concert_data in all_concerts_data.values():
        for artist in concert_data.get('artists', []):
            # Count ALL artists regardless of role - if you saw them play, they count!
            artist_id = artist.get('artist_id')
            if artist_id:
                artist_concert_counts[artist_id] += 1

    artists = []
    for artist_id, name in artist_names_map.items():
        concert_count = artist_concert_counts.get(artist_id, 0)
        if concert_count > 0:  # Only include artists with concerts
            artists.append({
                'id': artist_id,
                'name': name,
                'concert_count': concert_count
            })

    # Sort by concert count descending, then by name
    artists.sort(key=lambda x: (-x['concert_count'], x['name']))

    with open(output_dir / 'artists.json', 'w') as f:
        json.dump(artists, f, indent=2)
    print(f"   Exported {len(artists)} artists")

    # 4. Export venues list
    print("\n4. Exporting venues...")
    venues_ref = db.collection('venues')
    venues_docs = venues_ref.stream()

    # Count concerts per venue
    venue_concert_counts = defaultdict(int)
    venue_data_map = {}

    for venue_doc in venues_docs:
        venue_data = venue_doc.to_dict()
        venue_id = venue_doc.id
        venue_data_map[venue_id] = venue_data

    # Count concerts for each venue
    for concert_data in all_concerts_data.values():
        venue_id = concert_data.get('venue_id')
        if venue_id:
            venue_concert_counts[venue_id] += 1

    venues = []
    for venue_id, venue_data in venue_data_map.items():
        concert_count = venue_concert_counts.get(venue_id, 0)
        if concert_count > 0:  # Only include venues with concerts
            venues.append({
                'id': venue_id,
                'name': venue_data.get('canonical_name', ''),
                'city': venue_data.get('city', ''),
                'state': venue_data.get('state', ''),
                'concert_count': concert_count
            })

    # Sort by concert count descending, then by name
    venues.sort(key=lambda x: (-x['concert_count'], x['name']))

    with open(output_dir / 'venues.json', 'w') as f:
        json.dump(venues, f, indent=2)
    print(f"   Exported {len(venues)} venues")

    # 5. Export statistics
    print("\n5. Generating statistics...")

    total_concerts = len([c for c in all_concerts_data.values() if c.get('date')])
    concerts_with_setlists_count = len(concert_ids_with_setlists)

    # Count total songs
    setlists_docs = db.collection('setlists').stream()
    total_songs = 0
    for doc in setlists_docs:
        data = doc.to_dict()
        total_songs += data.get('song_count', 0)

    total_artists = len([a for a in artists if a['concert_count'] > 0])
    total_venues = len([v for v in venues if v['concert_count'] > 0])

    # Top artists (already sorted)
    top_artists = [{'name': a['name'], 'count': a['concert_count']}
                   for a in artists[:10]]

    # Top venues (already sorted)
    top_venues = [{'name': v['name'], 'count': v['concert_count']}
                  for v in venues[:10]]

    # Concerts by year
    concerts_by_year_dict = defaultdict(int)
    for concert_data in all_concerts_data.values():
        date_str = concert_data.get('date', '')
        if date_str and len(date_str) >= 4:
            year = int(date_str[:4])
            concerts_by_year_dict[year] += 1

    concerts_by_year = [{'year': year, 'count': count}
                        for year, count in sorted(concerts_by_year_dict.items(), reverse=True)]

    stats = {
        'total_concerts': total_concerts,
        'concerts_with_setlists': concerts_with_setlists_count,
        'total_songs': total_songs,
        'total_artists': total_artists,
        'total_venues': total_venues,
        'top_artists': top_artists,
        'top_venues': top_venues,
        'concerts_by_year': concerts_by_year,
        'generated_at': datetime.now().isoformat()
    }

    with open(output_dir / 'stats.json', 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"   Generated statistics")

    # 6. Export songs data
    print("\n6. Exporting songs...")

    # Collect all songs from all setlists
    song_counts = defaultdict(int)
    song_cover_counts = defaultdict(int)
    opening_songs = defaultdict(lambda: defaultdict(int))
    closing_songs = defaultdict(lambda: defaultdict(int))
    encore_songs = defaultdict(lambda: defaultdict(int))

    setlists_docs = db.collection('setlists').stream()

    for setlist_doc in setlists_docs:
        setlist_data = setlist_doc.to_dict()
        concert_id = setlist_data.get('concert_id')

        if concert_id not in all_concerts_data:
            continue

        concert_data = all_concerts_data[concert_id]
        songs = setlist_data.get('songs', [])

        # Get primary artist for this concert
        primary_artists = [a for a in concert_data.get('artists', [])
                          if a.get('role') in ['headliner', 'festival_performer']]

        for song in songs:
            song_name = song.get('name', '')
            if not song_name:
                continue

            song_counts[song_name] += 1
            if song.get('is_cover'):
                song_cover_counts[song_name] += 1

            position = song.get('position', 0)
            encore = song.get('encore', 0)
            set_name = song.get('set_name', '')

            # Track opening, closing, and encore songs by artist
            for artist in primary_artists:
                artist_name = artist.get('artist_name', '')

                # Opening song (position 1, main set)
                if position == 1 and (set_name in ['Main Set', ''] or encore == 0):
                    opening_songs[artist_name][song_name] += 1

                # Encore songs
                if encore > 0:
                    encore_songs[artist_name][song_name] += 1

        # Find closing songs (last song before encore for each setlist)
        non_encore_songs = [s for s in songs if s.get('encore', 0) == 0]
        if non_encore_songs:
            last_song = max(non_encore_songs, key=lambda s: s.get('position', 0))
            last_song_name = last_song.get('name', '')
            if last_song_name:
                for artist in primary_artists:
                    artist_name = artist.get('artist_name', '')
                    closing_songs[artist_name][last_song_name] += 1

    # Format songs data
    all_songs = []
    for song_name, times_heard in song_counts.items():
        times_as_cover = song_cover_counts.get(song_name, 0)
        all_songs.append({
            'name': song_name,
            'times_heard': times_heard,
            'is_mostly_cover': times_as_cover > times_heard / 2
        })

    all_songs.sort(key=lambda x: x['times_heard'], reverse=True)

    # Format opening songs by artist (filter to 2+ times)
    opening_songs_by_artist = {}
    for artist, songs_dict in opening_songs.items():
        filtered = [{'song': song, 'times': times}
                   for song, times in songs_dict.items() if times >= 2]
        if filtered:
            filtered.sort(key=lambda x: x['times'], reverse=True)
            opening_songs_by_artist[artist] = filtered

    # Format closing songs by artist (filter to 2+ times)
    closing_songs_by_artist = {}
    for artist, songs_dict in closing_songs.items():
        filtered = [{'song': song, 'times': times}
                   for song, times in songs_dict.items() if times >= 2]
        if filtered:
            filtered.sort(key=lambda x: x['times'], reverse=True)
            closing_songs_by_artist[artist] = filtered

    # Format encore songs by artist (filter to 2+ times)
    encore_songs_by_artist = {}
    for artist, songs_dict in encore_songs.items():
        filtered = [{'song': song, 'times': times}
                   for song, times in songs_dict.items() if times >= 2]
        if filtered:
            filtered.sort(key=lambda x: x['times'], reverse=True)
            encore_songs_by_artist[artist] = filtered

    songs_data = {
        'all_songs': all_songs,
        'opening_songs_by_artist': opening_songs_by_artist,
        'closing_songs_by_artist': closing_songs_by_artist,
        'encore_songs_by_artist': encore_songs_by_artist,
        'total_unique_songs': len(all_songs)
    }

    with open(output_dir / 'songs.json', 'w') as f:
        json.dump(songs_data, f, indent=2)
    print(f"   Exported {len(all_songs)} unique songs")

    # 7. Export venue details (one file per venue with concerts)
    print("\n7. Exporting venue details...")
    venue_details_dir = output_dir / 'venue_details'
    venue_details_dir.mkdir(exist_ok=True)

    for venue_id, venue_data in venue_data_map.items():
        # Get all concerts at this venue
        concerts_at_venue = []
        for concert_id, concert_data in all_concerts_data.items():
            if concert_data.get('venue_id') == venue_id:
                # Get primary artist names
                primary_artists = [a for a in concert_data.get('artists', [])
                                 if a.get('role') in ['headliner', 'festival_performer']]
                artist_names = ', '.join([a.get('artist_name', '') for a in primary_artists])

                concerts_at_venue.append({
                    'id': concert_id,
                    'show_number': concert_data.get('show_number'),
                    'date': concert_data.get('date', ''),
                    'festival_name': concert_data.get('festival_name'),
                    'artists': artist_names,
                    'has_setlist': concert_data.get('has_setlist', False),
                    'opening_song': concert_data.get('opening_song'),
                    'closing_song': concert_data.get('closing_song')
                })

        if not concerts_at_venue:
            continue

        # Sort by date descending
        concerts_at_venue.sort(key=lambda x: x['date'], reverse=True)

        venue_detail = {
            'id': venue_id,
            'name': venue_data.get('canonical_name', ''),
            'city': venue_data.get('city', ''),
            'state': venue_data.get('state', ''),
            'concert_count': len(concerts_at_venue),
            'concerts': concerts_at_venue
        }

        with open(venue_details_dir / f'{venue_id}.json', 'w') as f:
            json.dump(venue_detail, f, indent=2)

    print(f"   Exported {len(venue_data_map)} venue details")

    # 8. Export artist details (one file per artist with concerts)
    print("\n8. Exporting artist details...")
    artist_details_dir = output_dir / 'artist_details'
    artist_details_dir.mkdir(exist_ok=True)

    # Collect song data per artist
    artist_songs = defaultdict(lambda: defaultdict(int))

    setlists_docs = db.collection('setlists').stream()
    for setlist_doc in setlists_docs:
        setlist_data = setlist_doc.to_dict()
        concert_id = setlist_data.get('concert_id')

        if concert_id not in all_concerts_data:
            continue

        concert_data = all_concerts_data[concert_id]
        primary_artists = [a for a in concert_data.get('artists', [])
                          if a.get('role') in ['headliner', 'festival_performer']]

        for song in setlist_data.get('songs', []):
            song_name = song.get('name', '')
            if song_name:
                for artist in primary_artists:
                    artist_id = artist.get('artist_id')
                    if artist_id:
                        artist_songs[artist_id][song_name] += 1

    for artist_id, artist_name in artist_names_map.items():
        # Get all concerts for this artist
        concerts_for_artist = []
        for concert_id, concert_data in all_concerts_data.items():
            for artist in concert_data.get('artists', []):
                if artist.get('artist_id') == artist_id:
                    # Include ALL roles (headliner, opener, festival_performer)
                    concerts_for_artist.append({
                        'id': concert_id,
                        'show_number': concert_data.get('show_number'),
                        'date': concert_data.get('date', ''),
                        'festival_name': concert_data.get('festival_name'),
                        'venue': concert_data.get('venue_name', ''),
                        'city': concert_data.get('city', ''),
                        'state': concert_data.get('state', ''),
                        'role': artist.get('role', 'headliner'),  # Include role to show on artist page
                        'has_setlist': concert_data.get('has_setlist', False),
                        'opening_song': concert_data.get('opening_song'),
                        'closing_song': concert_data.get('closing_song')
                    })
                    break

        if not concerts_for_artist:
            continue

        # Sort by date descending
        concerts_for_artist.sort(key=lambda x: x['date'], reverse=True)

        # Get top songs
        songs_dict = artist_songs.get(artist_id, {})
        top_songs = [{'name': song, 'times_played': count}
                    for song, count in sorted(songs_dict.items(),
                                             key=lambda x: x[1], reverse=True)[:10]]

        artist_detail = {
            'id': artist_id,
            'name': artist_name,
            'concert_count': len(concerts_for_artist),
            'top_songs': top_songs,
            'concerts': concerts_for_artist
        }

        with open(artist_details_dir / f'{artist_id}.json', 'w') as f:
            json.dump(artist_detail, f, indent=2)

    print(f"   Exported {len(artist_names_map)} artist details")

    print("\n" + "=" * 60)
    print("Export complete!")
    print(f"Files written to: {output_dir}")


if __name__ == "__main__":
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    output_dir = project_root / "website" / "data"

    export_to_json(output_dir)
