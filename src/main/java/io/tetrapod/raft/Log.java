package io.tetrapod.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A raft log is the backbone of the raft algorithm. It stores an ordered list of commands that have been agreed upon by consensus, as well
 * as the tentative list of future commands yet to be confirmed.
 * 
 * 
 * <ul>
 * <li>TODO: Add a proper file lock so we can ensure only one raft process to a raft-dir</li>
 * <li>TODO: Make config constants configurable
 * </ul>
 * 
 */
public class Log<T extends StateMachine<T>> {

   public static final Logger   logger           = LoggerFactory.getLogger(Log.class);

   public static final int      LOG_FILE_VERSION = 3;

   /**
    * The log's in-memory buffer of log entries
    */
   private final List<Entry<T>> entries          = new ArrayList<>();

   private final Config         config;
   private FileLock             lock;

   /**
    * Our current journal file's output stream
    */
   private DataOutputStream     out;
   private boolean              running          = true;

   // We keep some key index & term variables that may or 
   // may not be in our buffer and are accessed frequently:

   private long                 snapshotIndex    = 0;
   private long                 snapshotTerm     = 0;
   private long                 firstIndex       = 0;
   private long                 firstTerm        = 0;
   private long                 lastIndex        = 0;
   private long                 lastTerm         = 0;
   private long                 commitIndex      = 0;

   /**
    * The state machine we are coordinating via raft
    */
   private final T              stateMachine;

   public Log(Config config, T stateMachine) throws IOException {
      this.stateMachine = stateMachine;
      this.config = config;
      this.config.getLogDir().mkdirs();

      // obtain the raft logs lock file
      obtainFileLock();

      // restore our state to the last snapshot
      loadSnapshot();

      // load all subsequent entries in our log
      replayLogs();

      // apply entries to our state machine
      updateStateMachine();

      // fire up our thread for writing log files 
      final Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            writeLoop();
         }
      }, "Raft Log Writer");
      t.start();
   }

   public synchronized T getStateMachine() {
      return stateMachine;
   }

   /**
    * Attempts to append the log entry to our log.
    * 
    * @return true if the entry was successfully appended to the log, or was already in our log to begin with
    */
   public synchronized boolean append(Entry<T> entry) {
      assert entry != null;
      // check if the entry is already in our log
      if (entry.index <= lastIndex) {
         //assert entry.index >= commitIndex : entry.index + " >= " + commitIndex;
         if (getTerm(entry.index) != entry.term) {
            logger.warn("Log is conflicted at {} : {} ", entry, getTerm(entry.index));
            wipeConflictedEntries(entry.index);
         } else {
            return true; // we already have this entry
         }
      }

      // validate that this is an acceptable entry to append next
      if (entry.index == lastIndex + 1 && entry.term >= lastTerm) {

         //logger.info("### APPENDING {} {} - {}", entry.term, entry.index, entry.command);

         // append to log
         entries.add(entry);

         // update our indexes
         if (firstIndex == 0) {
            assert (entries.size() == 1);
            firstIndex = entry.index;
            firstTerm = entry.term;

            logger.info("Setting First Index = {} ({})", firstIndex, entry.index);
         }
         lastIndex = entry.index;
         lastTerm = entry.term;

         return true;
      }

      return false;
   }

   /**
    * Append a new command to the log. Should only be called by a Leader
    */
   public synchronized Entry<T> append(long term, Command<T> command) {
      final Entry<T> e = new Entry<T>(term, lastIndex + 1, command);
      if (append(e)) {
         return e;
      } else {
         return null;
      }
   }

   /**
    * Get an entry from our log, by index
    */
   public synchronized Entry<T> getEntry(long index) {
      if (index > 0 && index <= lastIndex) {
         if (index >= firstIndex && entries.size() > 0) {

            assert index - firstIndex < Integer.MAX_VALUE;
            assert firstIndex == entries.get(0).index;
            //assert (index - firstIndex) < entries.size() : "index=" + index + ", first=" + firstIndex;
            final Entry<T> e = entries.get((int) (index - firstIndex));
            assert e.index == index;
            return e;
         } else {
            try {
               return getEntryFromDisk(index);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }
      }
      return null; // we don't have it!
   }

   /**
    * Fetch entries from fromIndex, up to maxEntries. Returns all or none.
    */
   public Entry<T>[] getEntries(long fromIndex, int maxEntries) {
      if (fromIndex > lastIndex) {
         return null;
      }
      @SuppressWarnings("unchecked")
      final Entry<T>[] list = (Entry<T>[]) new Entry<?>[(int) Math.min(maxEntries, (lastIndex - fromIndex) + 1)];
      for (int i = 0; i < list.length; i++) {
         list[i] = getEntry(fromIndex + i);
         if (list[i] == null) {
            logger.warn("Could not find log entry {}", fromIndex + i);
            return null;
         }
      }
      return list;
   }

   /**
    * Get the term for an entry in our log
    */
   public long getTerm(long index) {
      if (index == 0) {
         return 0;
      }
      if (index == stateMachine.getPrevIndex()) {
         return stateMachine.getPrevTerm();
      }
      if (index == stateMachine.getIndex()) {
         return stateMachine.getTerm();
      }
      if (index == snapshotIndex) {
         return snapshotTerm;
      }
      final Entry<T> e = getEntry(index);
      if (e == null) {
         logger.error("Could not find entry in log for {}", index);
      }
      return e.term;
   }

   /**
    * Deletes all uncommitted entries after a certain index
    */
   public synchronized void wipeConflictedEntries(long index) {
      assert index > snapshotIndex;
      if (index <= commitIndex) {
         stop();
         throw new RuntimeException("Can't restore conflicted index already written to disk: " + index);
      }

      // we have a conflict -- we need to throw away all entries from our log from this point on
      while (lastIndex >= index) {
         entries.remove((int) (lastIndex-- - firstIndex));
      }
      if (lastIndex > 0) {
         lastTerm = getTerm(lastIndex);
      } else {
         lastTerm = 0;
      }
   }

   public List<Entry<T>> getEntries() {
      return entries;
   }

   public File getLogDirectory() {
      return config.getLogDir();
   }

   public synchronized long getFirstIndex() {
      return firstIndex;
   }

   public synchronized long getFirstTerm() {
      return firstTerm;
   }

   public synchronized long getLastIndex() {
      return lastIndex;
   }

   public synchronized long getLastTerm() {
      return lastTerm;
   }

   public synchronized long getCommitIndex() {
      return commitIndex;
   }

   public synchronized void setCommitIndex(long index) {
      commitIndex = index;
   }

   public synchronized long getStateMachineIndex() {
      return stateMachine.getIndex();
   }

   /**
    * See if our log is consistent with the purported leader
    * 
    * @return false if log doesn't contain an entry at index whose term matches
    */
   public boolean isConsistentWith(final long index, final long term) {
      if (index == 0 && term == 0 || index > lastIndex) {
         return true;
      }
      if (index == snapshotIndex && term == snapshotTerm) {
         return true;
      }
      final Entry<T> entry = getEntry(index);
      if (entry == null) {
         if (index == stateMachine.getPrevIndex()) {
            return term == stateMachine.getPrevTerm();
         }
      }

      return (entry != null && entry.term == term);
   }

   public synchronized boolean isRunning() {
      return running;
   }

   public synchronized void stop() {
      logger.info("Stopping log...");
      running = false;
      try {
         updateStateMachine();
         if (out != null) {
            out.close();
            out = null;
         }
         if (lock != null) {
            lock.release();
         }
         logger.info("commitIndex = {}, lastIndex = {}", commitIndex, lastIndex);
      } catch (Throwable t) {
         logger.error(t.getMessage(), t);
      }

   }

   private void writeLoop() {
      while (isRunning()) {
         try {
            updateStateMachine();
            compact();
            if (out != null) {
               out.flush();
            }
            synchronized (this) {
               wait(100);
            }
         } catch (Exception t) {
            logger.error(t.getMessage(), t);
            stop();
         }
      }
   }

   @SuppressWarnings("resource")
   private void obtainFileLock() throws IOException {
      File lockFile = new File(getLogDirectory(), "lock");
      FileOutputStream stream = new FileOutputStream(lockFile);
      lock = stream.getChannel().tryLock();
      if (lock == null || !lock.isValid()) {
         throw new IOException("File lock held by another process: " + lockFile);
      }
      // purposefully kept open for lifetime of jvm
   }

   /**
    * Get the canonical file name for this index
    * 
    * @throws IOException
    */
   private File getFile(long index, boolean forReading) throws IOException {
      long firstIndexInFile = (index / config.getEntriesPerFile()) * config.getEntriesPerFile();
      File file = new File(getLogDirectory(), String.format("%016X.log", firstIndexInFile));
      if (forReading) {
         // if the config's entriesPerFile has changed, we need to scan files to find the right one

         // if the file is cached, we can do a quick check
         synchronized (entryFileCache) {
            final List<Entry<T>> list = entryFileCache.get(file.getCanonicalPath());
            if (list != null && !list.isEmpty()) {
               if (list.get(0).index <= index && list.get(list.size() - 1).index >= index) {
                  return file;
               }
            }
         }

         File bestFile = null;
         long bestIndex = 0;
         for (File f : getLogDirectory().listFiles()) {
            if (f.getName().matches("[A-F0-9]{16}\\.log")) {
               long i = Long.parseLong(f.getName().replace(".log", ""), 16);
               if (i <= index && i > bestIndex) {
                  bestFile = f;
                  bestIndex = i;
               }
            }
         }
         if (bestFile != null) {
            //logger.info("Best guess for file containing {} is {}", index, bestFile);
            file = bestFile;
         }
      }
      return file;
   }

   private synchronized void ensureCorrectLogFile(long index) throws IOException {
      if (index % config.getEntriesPerFile() == 0) {
         if (out != null) {
            out.close();
            out = null;
         }
      }
      if (out == null) {
         File file = getFile(index, false);
         if (file.exists()) {
            file.renameTo(new File(getLogDirectory(), "old." + file.getName()));
         }
         logger.info("Raft Log File : {}", file.getAbsolutePath());
         out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
         out.writeInt(LOG_FILE_VERSION);
      }
   }

   /**
    * Applies committed log entries to our state machine until it is at the given index
    */
   protected synchronized void updateStateMachine() {
      try {
         synchronized (stateMachine) {
            while (commitIndex > stateMachine.getIndex()) {
               final Entry<T> e = getEntry(stateMachine.getIndex() + 1);
               assert (e != null);
               assert (e.index == stateMachine.getIndex() + 1);
               stateMachine.apply(e);
               ensureCorrectLogFile(e.index);
               e.write(out);
               if (e.command.getCommandType() == StateMachine.COMMAND_ID_NEW_TERM) {
                  logger.info("Writing new term {}", e);
               }
               if ((e.index % config.getEntriesPerSnapshot()) == 0) {
                  saveSnapshot();
               }
            }
         }
      } catch (IOException e) {
         logger.error(e.getMessage(), e);
         running = false; // revisit this, but should probably halt
      }
   }

   protected synchronized void loadSnapshot() throws IOException {
      File file = new File(getLogDirectory(), "raft.snapshot");
      if (file.exists()) {
         logger.info("Loading snapshot {} ", file);
         stateMachine.readSnapshot(file);
         logger.info("Loaded snapshot @ {}:{}", stateMachine.getTerm(), stateMachine.getIndex());
         commitIndex = snapshotIndex = lastIndex = stateMachine.getIndex();
         snapshotTerm = lastTerm = stateMachine.getTerm();
         firstIndex = 0;
         firstTerm = 0;
         entries.clear();
         entryFileCache.clear();
      }
   }

   /**
    * Read and apply all available entries in the log from disk
    * 
    * @throws FileNotFoundException
    */
   private synchronized void replayLogs() throws IOException {
      Entry<T> entry = null;
      do {
         entry = getEntryFromDisk(stateMachine.getIndex() + 1);
         if (entry != null) {
            stateMachine.apply(entry);
         }
      } while (entry != null);

      // get the most recent file of entries
      final List<Entry<T>> list = loadLogFile(getFile(stateMachine.getIndex(), true));
      if (list != null && list.size() > 0) {
         assert (entries.size() == 0);
         entries.addAll(list);
         firstIndex = entries.get(0).index;
         firstTerm = entries.get(0).term;
         lastIndex = entries.get(entries.size() - 1).index;
         lastTerm = entries.get(entries.size() - 1).term;
         // TODO: rename existing file in case of failure
         // re-write out the last file
         out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(getFile(firstIndex, true), false)));
         out.writeInt(LOG_FILE_VERSION);
         for (Entry<T> e : list) {
            e.write(out);
         }
         out.flush();
         commitIndex = lastIndex;
         logger.info("Log First Index = {}, Last Index = {}", firstIndex, lastIndex);
      }
      synchronized (entryFileCache) {
         entryFileCache.clear();
      }
   }

   /**
    * An LRU cache of entries loaded from disk
    */
   @SuppressWarnings("serial")
   private final Map<String, List<Entry<T>>> entryFileCache = new LinkedHashMap<String, List<Entry<T>>>(3, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, List<Entry<T>>> eldest) {
         return size() > 2;
      }
   };

   private Entry<T> getEntryFromDisk(long index) throws IOException {
      File file = getFile(index, true);
      if (file.exists()) {
         List<Entry<T>> list = loadLogFile(file);
         if (list != null && list.size() > 0) {
            int i = (int) (index - list.get(0).index);
            if (i >= 0 && i < list.size()) {
               assert list.get(i).index == index;
               return list.get(i);
            }
         }
      } else {
         logger.info("Could not find file {}", file);
      }
      return null;
   }

   public List<Entry<T>> loadLogFile(File file) throws IOException {
      synchronized (entryFileCache) {
         List<Entry<T>> list = entryFileCache.get(file.getCanonicalPath());
         if (list == null) {
            list = new ArrayList<>();
            if (file.exists()) {
               logger.info("Loading Log File {}", file);
               try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                  final int version = in.readInt();
                  assert (version <= LOG_FILE_VERSION);
                  Entry<T> last = null;
                  while (true) {
                     final Entry<T> e = new Entry<T>(in, version, stateMachine);
                     if (last != null) {
                        if (e.index != last.index + 1) {
                           logger.error("Log File {} is inconsistent. {} followed by {}", file, last, e);
                        }

                        assert e.term >= last.term;
                        assert e.index == last.index + 1;
                     }
                     list.add(e);
                     last = e;
                  }
               } catch (EOFException t) {
                  logger.debug("Read {} from {}", list.size(), file);
               }
            }
            entryFileCache.put(file.getCanonicalPath(), list);
         }
         return list;
      }
   }

   /**
    * Discards entries from our buffer that we no longer need to store in memory
    */
   private synchronized void compact() {
      if (entries.size() > config.getEntriesPerFile() * 2) {

         if (firstIndex > commitIndex || firstIndex > stateMachine.getIndex() || firstIndex > lastIndex - config.getEntriesPerFile()) {
            return;
         }

         logger.info("Compacting log size = {}", entries.size());
         List<Entry<T>> entriesToKeep = new ArrayList<>();
         for (Entry<T> e : entries) {
            if (e.index > commitIndex || e.index > stateMachine.getIndex() || e.index > lastIndex - config.getEntriesPerFile()) {
               entriesToKeep.add(e);
            }
         }
         entries.clear();
         entries.addAll(entriesToKeep);
         Entry<T> first = entries.get(0);
         firstIndex = first.index;
         firstTerm = first.term;
         logger.info("Compacted log new size = {}, firstIndex = {}", entries.size(), firstIndex);
      }
   }

   private void archiveOldLogFiles() throws IOException {
      if (config.getDeleteOldFiles()) {
         final File archiveDir = new File(getLogDirectory(), "archived");
         archiveDir.mkdir();
         long index = commitIndex - (config.getEntriesPerSnapshot() * 4);
         while (index >= 0) {
            logger.info(" Checking ::  {}", Long.toHexString(index));
            File file = getFile(index, true);
            if (file.exists()) {
               logger.info("Archiving old log file {}", file);
               file.renameTo(new File(archiveDir, file.getName()));
               // TODO: Archive into larger log files
            } else {
               break; // done archiving
            }
            index -= config.getEntriesPerFile();
         }
         final Pattern p = Pattern.compile("raft\\.([0-9A-F]{16})\\.snapshot");
         for (File file : getLogDirectory().listFiles()) {
            Matcher m = p.matcher(file.getName());
            if (m.matches()) {
               final long snapIndex = Long.parseLong(m.group(1), 16);
               logger.info("{} Checking {}: {}", Long.toHexString(index), file, Long.toHexString(snapIndex));
               if (snapIndex < index) {
                  if (snapIndex % (config.getEntriesPerSnapshot() * 16) == 0) {
                     logger.info("Archiving old snapshot file {}", file);
                     file.renameTo(new File(archiveDir, file.getName()));
                  } else {
                     // otherwise delete the older ones
                     logger.info("Deleting old snapshot file {}", file);
                     file.delete();
                  }
               }
            }
         }
      }
   }

   /**
    * Currently is a pause-the-world snapshot
    */
   public long saveSnapshot() throws IOException {
      // currently pauses the world to save a snapshot
      synchronized (stateMachine) {
         final File openFile = new File(getLogDirectory(), "open.snapshot");
         logger.info(String.format("Saving snapshot @ %016X", stateMachine.getIndex()));
         stateMachine.writeSnapshot(openFile, getTerm(stateMachine.getPrevIndex()));
         File file = new File(getLogDirectory(), "raft.snapshot");
         if (file.exists()) {
            final long oldIndex = StateMachine.getSnapshotIndex(file);
            file.renameTo(new File(getLogDirectory(), String.format("raft.%016X.snapshot", oldIndex)));
         }
         openFile.renameTo(file);
         archiveOldLogFiles();
         return stateMachine.getIndex();
      }
   }

}
