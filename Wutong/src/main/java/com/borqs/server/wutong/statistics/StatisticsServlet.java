package com.borqs.server.wutong.statistics;


import com.borqs.server.base.data.Record;
import com.borqs.server.base.web.QueryParams;
import com.borqs.server.base.web.webmethod.WebMethod;
import com.borqs.server.base.web.webmethod.WebMethodServlet;
import com.borqs.server.wutong.GlobalLogics;
import org.apache.avro.AvroRemoteException;

import javax.servlet.ServletException;
import java.util.Timer;
import java.util.TimerTask;

public class StatisticsServlet extends WebMethodServlet {
    private Record statistics = new Record();
    private Timer timer;
    private StatisticsTask task;
    private final long interval = 60 * 1000;

    @Override
    public void init() throws ServletException {
        super.init();
        timer = new Timer();
        task = new StatisticsTask();
        timer.schedule(task, interval, interval);
    }
    private class StatisticsTask extends TimerTask {

        @Override
        public void run() {
            try {
                saveStatistics();
            } catch (AvroRemoteException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean saveStatistics() throws AvroRemoteException {

        StatisticsLogic statisticsLogic = GlobalLogics.getStatisticsLogic();
//         L.debug("Begin save statistics");
        boolean r = statisticsLogic.save(statistics);
//         L.debug("End save statistics");
        statistics.clear();
        return r;
    }

    @WebMethod("internal/statistics")
    public Record httpCallStatistics(QueryParams qp) {
        String api = qp.checkGetString("api");
//        L.debug("api: " + api);
        long increment = statistics.getInt(api, 0L);
        increment++;
        statistics.put(api, increment);
        return Record.of(api, increment);
    }
}
