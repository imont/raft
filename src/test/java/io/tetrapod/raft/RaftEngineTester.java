package io.tetrapod.raft;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;

/**
 * Runs a full system simulation with fake RPC
 */
public class RaftEngineTester implements RaftRPC<TestStateMachine> {

    public static final Logger logger = LoggerFactory.getLogger(RaftEngineTester.class);
    private static final int NUM_PEERS = 3;

    private static ScheduledExecutorService executor;
    private static File[] logDirs = new File[NUM_PEERS];

    @Before
    public void createTestDirectories() throws IOException {
        for (int i = 0; i < NUM_PEERS; i++) {
            File parent = Files.createTempDirectory("raft").toFile();
            logDirs[i] = new File(parent, "logs/test-" + (i + 1));
            if (!logDirs[i].mkdirs()) {
                throw new RuntimeException("Failed to create log dir " + logDirs[i]);
            }
        }
        executor = Executors.newScheduledThreadPool(NUM_PEERS);
    }

    @After
    public void shutdown() {
        executor.shutdownNow();
    }

    private static int randomDelay() {
        return 1 + (int) (Math.random() * 10);
    }

    private Map<Integer, RaftEngine<TestStateMachine>> rafts = new HashMap<>();

    @Test
    public void willElectLeaderInitially() throws Exception {
        // given
        startEverything(3);

        // when
        // it's running

        // then
        assertLeaderElected();
    }

    @Ignore("WIP")
    @Test
    public void willElectNewLeaderWhenCurrentOneLeavesCluster() throws IOException {

    }

    @Test
    public void willElectNewLeaderWhenCurrentOneDisconnectsTemporarily() throws IOException {
        // given
        startEverything(3);
        assertLeaderElected();
        RaftEngine<TestStateMachine> leader = getLeader();

        // when
        // it's running
        leader.stop();

        // then
        assertLeaderElected();
        RaftEngine<TestStateMachine> newLeader = getLeader();
        assertThat(leader.getPeerId(), not(equalTo(newLeader.getPeerId())));
    }

    @Ignore("WIP")
    @Test
    public void willNotElectLeaderIfNotQuorum() throws Exception {
        // given
        startEverything(3);
        assertLeaderElected();
        RaftEngine<TestStateMachine> leader = getLeader();
        RaftEngine<TestStateMachine> follower = getFirstFollower();

        // when
        // it's running
        leader.stop();
        follower.stop();

        // then
        Thread.sleep(1000);
        assertThat(getLeader(), is(nullValue()));
    }

    private RaftEngine<TestStateMachine> getLeader() {
        for (Map.Entry<Integer, RaftEngine<TestStateMachine>> entry : rafts.entrySet()) {
            if (entry.getValue().getRole() == RaftEngine.Role.Leader) {
                return entry.getValue();
            }
        }
        return null;
    }

    private RaftEngine<TestStateMachine> getFirstFollower() {
        for (Map.Entry<Integer, RaftEngine<TestStateMachine>> entry : rafts.entrySet()) {
            if (entry.getValue().getRole() == RaftEngine.Role.Follower) {
                return entry.getValue();
            }
        }
        return null;
    }


    private void startEverything(int numPeers) throws IOException {
        // given
        for (int i = 1; i <= numPeers; i++) {
            Config cfg = new Config()
                    .setLogDir(logDirs[i - 1])
                    .setClusterName("TEST")
                    .setElectionTimeoutFixedMillis(100);
            RaftEngine<TestStateMachine> raft = new RaftEngine<TestStateMachine>(cfg, new TestStateMachine.Factory(), this);
            raft.setPeerId(i);
            for (int j = 1; j <= numPeers; j++) {
                if (j != i) {
                    raft.addPeer(j);
                }
            }
            rafts.put(i, raft);
        }

        // when
        for (RaftEngine<?> raft : rafts.values()) {
            raft.start(raft.getPeerId());
        }
    }

    private void assertLeaderElected() {
        await().atMost(5, TimeUnit.SECONDS).until(new Callable<Map<Integer, RaftEngine<TestStateMachine>>>() {
            @Override
            public Map<Integer, RaftEngine<TestStateMachine>> call() throws Exception {
                return rafts;
            }
        }, leaderElected());
    }

    @Override
    public void sendRequestVote(final String clusterName, int peerId, final long term, final int candidateId, final long lastLogIndex,
                                final long lastLogTerm, final VoteResponseHandler handler) {
        final RaftEngine<?> r = rafts.get(peerId);
        if (r != null && r.getRole() != RaftEngine.Role.Leaving) {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        r.handleVoteRequest(clusterName, term, candidateId, lastLogIndex, lastLogTerm, handler);
                    } catch (Throwable t) {
                        logger.error(t.getMessage(), t);
                    }
                }
            }, randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void sendAppendEntries(int peerId, final long term, final int leaderId, final long prevLogIndex, final long prevLogTerm,
                                  final Entry<TestStateMachine>[] entries, final long leaderCommit, final AppendEntriesResponseHandler handler) {
        final RaftEngine<TestStateMachine> r = rafts.get(peerId);
        if (r != null && r.getRole() != RaftEngine.Role.Leaving) {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        r.handleAppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, handler);
                    } catch (Throwable t) {
                        logger.error(t.getMessage(), t);
                    }
                }
            }, randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void sendInstallSnapshot(int peerId, final long term, final long index, final long length, final int partSize, final int part,
                                    final byte[] data, final InstallSnapshotResponseHandler handler) {
        final RaftEngine<TestStateMachine> r = rafts.get(peerId);
        if (r != null && r.getRole() != RaftEngine.Role.Leaving) {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        r.handleInstallSnapshotRequest(term, index, length, partSize, part, data, handler);
                    } catch (Throwable t) {
                        logger.error(t.getMessage(), t);
                    }
                }
            }, randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void sendIssueCommand(int peerId, final Command<TestStateMachine> command, final ClientResponseHandler<TestStateMachine> handler) {
        final RaftEngine<TestStateMachine> r = rafts.get(peerId);
        if (r != null && r.getRole() != RaftEngine.Role.Leaving) {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        r.handleClientRequest(command, handler);
                    } catch (Throwable t) {
                        logger.error(t.getMessage(), t);
                    }
                }
            }, randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    private Matcher<Map<Integer, RaftEngine<TestStateMachine>>> leaderElected() {
        return new TypeSafeMatcher<Map<Integer, RaftEngine<TestStateMachine>>>() {
            @Override
            protected boolean matchesSafely(final Map<Integer, RaftEngine<TestStateMachine>> map) {
                for (Map.Entry<Integer, RaftEngine<TestStateMachine>> entry : map.entrySet()) {
                    if (entry.getValue().getRole() == RaftEngine.Role.Leader) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("leader to be elected");
            }
        };
    }

}
