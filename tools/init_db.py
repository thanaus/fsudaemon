"""
PostgreSQL database initialization script
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
from config import load_config


async def init_database():
    """Initializes database schema"""
    
    print("🔧 Initializing database...")
    
    # Load configuration
    config = load_config()
    
    # Connect to PostgreSQL
    conn = await asyncpg.connect(config.get_db_dsn())
    
    try:
        # 1. Create audit_points table
        print("📋 Creating 'audit_points' table...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_points (
                id SERIAL PRIMARY KEY,
                bucket VARCHAR(255) NOT NULL,
                prefix VARCHAR(1024) NOT NULL,
                description TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                deleted_at TIMESTAMPTZ
            )
        """)
        
        # Indexes for audit_points
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_points_bucket 
            ON audit_points(bucket)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_points_deleted 
            ON audit_points(deleted_at) WHERE deleted_at IS NULL
        """)
        
        # 2. Create s3_events table
        print("📋 Creating 's3_events' table...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS s3_events (
                id BIGSERIAL PRIMARY KEY,
                event_time TIMESTAMPTZ NOT NULL,
                event_name VARCHAR(100) NOT NULL,
                bucket VARCHAR(255) NOT NULL,
                object_key VARCHAR(1024) NOT NULL,
                size BIGINT DEFAULT 0,
                version_id VARCHAR(255),
                audit_point_ids INTEGER[] NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW(),
                CHECK (size >= 0)
            )
        """)
        
        # Indexes for s3_events
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_s3_events_event_time 
            ON s3_events(event_time DESC)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_s3_events_bucket_key 
            ON s3_events(bucket, object_key)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_s3_events_event_name 
            ON s3_events(event_name)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_s3_events_received_at 
            ON s3_events(received_at DESC)
        """)

        # Uniqueness for idempotence: restart / SQS replay without duplicates
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_s3_events_dedup
            ON s3_events (bucket, object_key, event_time, event_name)
        """)
        
        # GIN index for fast array queries
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_s3_events_audit_points 
            ON s3_events USING GIN (audit_point_ids)
        """)
        
        # 3. Create trigger function to notify audit point changes
        print("📋 Creating NOTIFY trigger for audit_points...")
        await conn.execute("""
            CREATE OR REPLACE FUNCTION notify_audit_points_change()
            RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('audit_points_changed', json_build_object(
                    'action', TG_OP,
                    'id', COALESCE(NEW.id, OLD.id),
                    'bucket', COALESCE(NEW.bucket, OLD.bucket),
                    'prefix', COALESCE(NEW.prefix, OLD.prefix)
                )::text);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Create trigger on INSERT, UPDATE, DELETE
        await conn.execute("""
            DROP TRIGGER IF EXISTS audit_points_change_trigger ON audit_points;
        """)
        
        await conn.execute("""
            CREATE TRIGGER audit_points_change_trigger
            AFTER INSERT OR UPDATE OR DELETE ON audit_points
            FOR EACH ROW EXECUTE FUNCTION notify_audit_points_change();
        """)
        
        print("✅ Database initialized successfully!")
        print("\n📊 Tables created:")
        print("  - audit_points")
        print("  - s3_events (with audit_point_ids array)")
        print("\n🔔 NOTIFY Trigger:")
        print("  - audit_points_change_trigger → channel 'audit_points_changed'")
        print("\n📊 Indexes:")
        print("  - GIN index on audit_point_ids for fast array queries")
        print("  - UNIQUE index (bucket, object_key, event_time, event_name) for idempotence / no SQS duplicates")
        
        # Display statistics
        count_ap = await conn.fetchval("SELECT COUNT(*) FROM audit_points WHERE deleted_at IS NULL")
        count_ap_deleted = await conn.fetchval("SELECT COUNT(*) FROM audit_points WHERE deleted_at IS NOT NULL")
        count_ev = await conn.fetchval("SELECT COUNT(*) FROM s3_events")
        
        print(f"\n📈 Statistics:")
        print(f"  - Audit points: {count_ap} active, {count_ap_deleted} deleted")
        print(f"  - S3 events: {count_ev}")
        
    except Exception as e:
        print(f"❌ Initialization error: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(init_database())
