import { Database } from 'bun:sqlite';
import { existsSync, mkdirSync, renameSync } from 'fs';

export function cleanupEventsDB(): void {
  console.log('[Cleanup] Starting database cleanup...');
  
  try {
    const db = new Database('events.db');
    
    // Enable WAL mode for better performance
    db.exec('PRAGMA journal_mode = WAL');
    
    // Get initial size
    const initialSize = Bun.file('events.db').size;
    console.log(`[Cleanup] Initial DB size: ${(initialSize / 1024 / 1024).toFixed(2)} MB`);
    
    // 1. Remove events older than 7 days
    const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
    const deletedOld = db.prepare('DELETE FROM events WHERE timestamp < ?').run(sevenDaysAgo);
    console.log(`[Cleanup] Deleted ${deletedOld.changes} events older than 7 days`);
    
    // 2. Truncate large payloads (>10KB) older than 1 day
    const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
    const stmt = db.prepare(`
      UPDATE events 
      SET payload = json_object(
        'truncated', true, 
        'original_size', LENGTH(payload),
        'source_app', json_extract(payload, '$.source_app'),
        'session_id', json_extract(payload, '$.session_id'),
        'hook_event_name', json_extract(payload, '$.hook_event_name')
      )
      WHERE LENGTH(payload) > 10000 AND timestamp < ?
    `);
    const truncated = stmt.run(oneDayAgo);
    console.log(`[Cleanup] Truncated ${truncated.changes} large payloads`);
    
    // 3. Clean up orphaned theme shares (expired)
    const now = Date.now();
    const expiredShares = db.prepare(`
      DELETE FROM theme_shares 
      WHERE expiresAt IS NOT NULL AND expiresAt < ?
    `).run(now);
    console.log(`[Cleanup] Deleted ${expiredShares.changes} expired theme shares`);
    
    // 4. VACUUM to reclaim space
    console.log('[Cleanup] Running VACUUM to reclaim space...');
    db.exec('VACUUM');
    
    // 5. Optimize indexes
    console.log('[Cleanup] Reindexing...');
    db.exec('REINDEX');
    
    // 6. Analyze for query optimization
    db.exec('ANALYZE');
    
    db.close();
    
    // Check final size
    const finalSize = Bun.file('events.db').size;
    console.log(`[Cleanup] Final DB size: ${(finalSize / 1024 / 1024).toFixed(2)} MB`);
    console.log(`[Cleanup] Space saved: ${((initialSize - finalSize) / 1024 / 1024).toFixed(2)} MB`);
    
    // 7. Backup rotation if DB still > 50MB
    if (finalSize > 50 * 1024 * 1024) {
      console.log('[Cleanup] Database still large, creating backup...');
      
      // Ensure backups directory exists
      if (!existsSync('backups')) {
        mkdirSync('backups');
      }
      
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const backupName = `backups/events_${timestamp}.db`;
      renameSync('events.db', backupName);
      console.log(`[Cleanup] Created backup: ${backupName}`);
      
      // Reinitialize database with current schema
      const newDb = new Database('events.db');
      
      // Enable WAL mode
      newDb.exec('PRAGMA journal_mode = WAL');
      newDb.exec('PRAGMA synchronous = NORMAL');
      
      // Create events table with current schema
      newDb.exec(`
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_app TEXT NOT NULL,
          session_id TEXT NOT NULL,
          hook_event_type TEXT NOT NULL,
          payload TEXT NOT NULL,
          chat TEXT,
          summary TEXT,
          timestamp INTEGER NOT NULL
        )
      `);
      
      // Create themes table
      newDb.exec(`
        CREATE TABLE IF NOT EXISTS themes (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          displayName TEXT NOT NULL,
          description TEXT,
          colors TEXT NOT NULL,
          isPublic INTEGER NOT NULL DEFAULT 0,
          authorId TEXT,
          authorName TEXT,
          createdAt INTEGER NOT NULL,
          updatedAt INTEGER NOT NULL,
          tags TEXT,
          downloadCount INTEGER DEFAULT 0,
          rating REAL DEFAULT 0,
          ratingCount INTEGER DEFAULT 0
        )
      `);
      
      // Create theme shares table
      newDb.exec(`
        CREATE TABLE IF NOT EXISTS theme_shares (
          id TEXT PRIMARY KEY,
          themeId TEXT NOT NULL,
          shareToken TEXT NOT NULL UNIQUE,
          expiresAt INTEGER,
          isPublic INTEGER NOT NULL DEFAULT 0,
          allowedUsers TEXT,
          createdAt INTEGER NOT NULL,
          accessCount INTEGER DEFAULT 0,
          FOREIGN KEY (themeId) REFERENCES themes (id) ON DELETE CASCADE
        )
      `);
      
      // Create theme ratings table
      newDb.exec(`
        CREATE TABLE IF NOT EXISTS theme_ratings (
          id TEXT PRIMARY KEY,
          themeId TEXT NOT NULL,
          userId TEXT NOT NULL,
          rating INTEGER NOT NULL,
          comment TEXT,
          createdAt INTEGER NOT NULL,
          UNIQUE(themeId, userId),
          FOREIGN KEY (themeId) REFERENCES themes (id) ON DELETE CASCADE
        )
      `);
      
      // Create all indexes
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_source_app ON events(source_app)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_session_id ON events(session_id)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_hook_event_type ON events(hook_event_type)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_themes_name ON themes(name)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_themes_isPublic ON themes(isPublic)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_themes_createdAt ON themes(createdAt)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_theme_shares_token ON theme_shares(shareToken)');
      newDb.exec('CREATE INDEX IF NOT EXISTS idx_theme_ratings_theme ON theme_ratings(themeId)');
      
      newDb.close();
      
      console.log('[Cleanup] Created fresh database with complete schema');
    }
    
    console.log('[Cleanup] Database cleanup completed successfully');
  } catch (error) {
    console.error('[Cleanup] Error during cleanup:', error);
  }
}

// Get database statistics for monitoring
export function getDatabaseStats(): {
  size: number;
  sizeFormatted: string;
  totalEvents: number;
  eventsLast7Days: number;
  oldEvents: number;
  largePayloads: number;
  lastCleanup: string;
} {
  try {
    const db = new Database('events.db');
    
    // Get database size
    const size = Bun.file('events.db').size;
    const sizeFormatted = `${(size / 1024 / 1024).toFixed(2)} MB`;
    
    // Get event counts
    const totalEvents = db.prepare('SELECT COUNT(*) as count FROM events').get() as { count: number };
    
    const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
    const eventsLast7Days = db.prepare('SELECT COUNT(*) as count FROM events WHERE timestamp > ?').get(sevenDaysAgo) as { count: number };
    
    const oldEvents = db.prepare('SELECT COUNT(*) as count FROM events WHERE timestamp < ?').get(sevenDaysAgo) as { count: number };
    
    const largePayloads = db.prepare('SELECT COUNT(*) as count FROM events WHERE LENGTH(payload) > 10000').get() as { count: number };
    
    db.close();
    
    return {
      size,
      sizeFormatted,
      totalEvents: totalEvents.count,
      eventsLast7Days: eventsLast7Days.count,
      oldEvents: oldEvents.count,
      largePayloads: largePayloads.count,
      lastCleanup: new Date().toISOString() // This would be stored in a config/state file in production
    };
  } catch (error) {
    console.error('[Stats] Error getting database stats:', error);
    return {
      size: 0,
      sizeFormatted: '0 MB',
      totalEvents: 0,
      eventsLast7Days: 0,
      oldEvents: 0,
      largePayloads: 0,
      lastCleanup: 'Never'
    };
  }
}

// Export for manual execution
if (import.meta.main) {
  cleanupEventsDB();
}