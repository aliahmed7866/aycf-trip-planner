package uk.aycf.mobile;

import android.app.job.JobInfo;
import android.app.job.JobParameters;
import android.app.job.JobScheduler;
import android.app.job.JobService;
import android.content.ComponentName;
import android.content.Context;
import android.os.Handler;
import android.os.Looper;
import android.os.PersistableBundle;
import android.os.Process;
import android.util.Log;

public final class ScanJob extends JobService {
    private static final int TICK = 70, WORK = 71;
    private volatile JobParameters active;

    static void schedule(Context context) {
        JobScheduler scheduler = context.getSystemService(JobScheduler.class);
        if (scheduler.getPendingJob(TICK) == null) {
            scheduler.schedule(new JobInfo.Builder(TICK, new ComponentName(context, ScanJob.class))
                .setPeriodic(15 * 60_000L).setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY).setPersisted(true).build());
        }
    }

    static void request(Context context, String action) {
        JobScheduler scheduler = context.getSystemService(JobScheduler.class);
        if (scheduler.getPendingJob(WORK) != null) return;
        PersistableBundle options = new PersistableBundle();
        options.putString("action", action);
        JobInfo job = new JobInfo.Builder(WORK, new ComponentName(context, ScanJob.class))
            .setRequiredNetworkType(JobInfo.NETWORK_TYPE_ANY).setMinimumLatency(0)
            .setBackoffCriteria(15 * 60_000L, JobInfo.BACKOFF_POLICY_EXPONENTIAL)
            .setPersisted(true).setExtras(options).build();
        if (scheduler.schedule(job) != JobScheduler.RESULT_SUCCESS)
            throw new IllegalStateException("Android could not schedule the scan.");
    }

    @Override public boolean onStartJob(JobParameters params) {
        if (params.getJobId() == TICK) { request(this, "auto"); return false; }
        active = params;
        new Thread(() -> {
            boolean retry = false;
            try {
                retry = Engine.runtime(this).callAttr("run_job", params.getExtras().getString("action", "auto")).toBoolean();
            } catch (Exception err) {
                Log.w("AYCF", "Scan interrupted: " + err.getClass().getSimpleName());
                retry = true;
            }
            final boolean again = retry;
            new Handler(Looper.getMainLooper()).post(() -> {
                if (active == params) { active = null; jobFinished(params, again); }
            });
        }, "AYCF scan").start();
        return true;
    }

    @Override public boolean onStopJob(JobParameters params) {
        if (active != params) return false;
        active = null;
        // Python HTTP workers run only in this service process. SQLite commits survive
        // pre-emption and the resumed job reuses already-verified airport/date checks.
        new Handler(Looper.getMainLooper()).post(() -> Process.killProcess(Process.myPid()));
        return true;
    }
}
