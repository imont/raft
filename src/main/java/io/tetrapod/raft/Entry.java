package io.tetrapod.raft;

import java.io.*;

public class Entry<T extends StateMachine<T>> {

   final long       term;
   final long       index;
   final Command<T> command;

   public Entry(long term, long index, Command<T> command) {
      this.term = term;
      this.index = index;
      this.command = command;
   }

   public long getTerm() {
      return term;
   }

   public long getIndex() {
      return index;
   }

   public Command<T> getCommand() {
      return command;
   }

   /**
    * Read this command to from an input stream
    */
   public Entry(DataInputStream in, int fileVersion, T state) throws IOException {
      term = in.readLong();
      index = in.readLong();
      final int typeId = in.readInt();
      command = state.makeCommandById(typeId);
      if (command == null) {
         throw new IOException("Could not create command type " + typeId);
      }
      command.read(in, fileVersion);
   }

   /**
    * Writes this entry to an output stream
    */
   public void write(DataOutputStream out) throws IOException {
      out.writeLong(term);
      out.writeLong(index);
      assert command.getCommandType() != 0;
      out.writeInt(command.getCommandType());
      command.write(out);
   }

   @Override
   public String toString() {
      return String.format("Entry<%d:%d>", term, index);
   }

}
