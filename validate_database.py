#!/usr/bin/env python3

import duckdb
import sys
from pathlib import Path

def main():
    db_path = "output/wrestling.duckdb"
    
    if not Path(db_path).exists():
        print(f"❌ Database file {db_path} does not exist")
        return False
        
    try:
        print(f"🔍 Connecting to {db_path}...")
        conn = duckdb.connect(db_path)
        
        # Get tables
        print("📋 Checking tables...")
        tables_result = conn.execute("SHOW TABLES").fetchall()
        tables = [t[0] for t in tables_result]
        
        if not tables:
            print("❌ No tables found in database")
            return False
            
        print(f"✅ Found {len(tables)} tables: {', '.join(tables)}")
        
        # Check each table
        print("\n📊 Record counts:")
        total_records = 0
        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"   {table}: {count:,} records")
                total_records += count
            except Exception as e:
                print(f"   {table}: ❌ Error - {e}")
                
        print(f"\n📈 Total records across all tables: {total_records:,}")
        
        # Sample data from key tables
        if 'person' in tables:
            print("\n👤 Sample person records:")
            persons = conn.execute("SELECT * FROM person LIMIT 3").fetchall()
            for i, person in enumerate(persons, 1):
                print(f"   {i}. {person}")
                
        if 'match' in tables:
            print("\n🥊 Sample match records:")
            matches = conn.execute("SELECT * FROM match LIMIT 3").fetchall()
            for i, match in enumerate(matches, 1):
                print(f"   {i}. {match}")
                
        if 'participant_match' in tables:
            print("\n🏆 Sample participant_match records:")
            pm_records = conn.execute("SELECT * FROM participant_match LIMIT 3").fetchall()
            for i, pm in enumerate(pm_records, 1):
                print(f"   {i}. {pm}")
        
        # Test a complex query
        if all(t in tables for t in ['person', 'role', 'participant', 'participant_match', 'match']):
            print("\n🔍 Testing complex query (match with wrestler names):")
            query = """
            SELECT 
                p1.first_name || ' ' || p1.last_name as winner_name,
                p2.first_name || ' ' || p2.last_name as loser_name,
                m.round
            FROM participant_match pm1
            JOIN participant_match pm2 ON pm1.match_id = pm2.match_id AND pm1.participant_id != pm2.participant_id
            JOIN participant part1 ON pm1.participant_id = part1.participant_id
            JOIN participant part2 ON pm2.participant_id = part2.participant_id
            JOIN role r1 ON part1.role_id = r1.role_id
            JOIN role r2 ON part2.role_id = r2.role_id
            JOIN person p1 ON r1.person_id = p1.person_id
            JOIN person p2 ON r2.person_id = p2.person_id
            JOIN match m ON pm1.match_id = m.match_id
            WHERE pm1.is_winner = true AND pm2.is_winner = false
            LIMIT 3
            """
            try:
                results = conn.execute(query).fetchall()
                for i, result in enumerate(results, 1):
                    print(f"   {i}. {result[0]} beat {result[1]} in {result[2]}")
            except Exception as e:
                print(f"   ❌ Query failed: {e}")
        
        conn.close()
        print("\n✅ Database validation completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
