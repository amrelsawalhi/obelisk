import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../lib/api';
import type { IngestJobInfo } from '../lib/types';

interface IngestContextType {
  jobs: Record<string, IngestJobInfo>;
  activeJobId: string | null;
  activeJob: IngestJobInfo | null;
  isMinimized: boolean;
  isDrawerOpen: boolean;
  logs: Record<string, string[]>;
  activeLogs: string[];
  runningCount: number;
  startJob: (jobId: string, initialTitle?: string) => void;
  selectJob: (jobId: string) => void;
  minimizeDrawer: () => void;
  restoreDrawer: () => void;
  dismissJob: (jobId: string) => void;
  clearCompleted: () => void;
  cancelJob: (jobId: string) => Promise<void>;
  cancelAll: () => Promise<void>;
}

const IngestContext = createContext<IngestContextType | undefined>(undefined);

export const IngestProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [jobs, setJobs] = useState<Record<string, IngestJobInfo>>({});
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [isMinimized, setIsMinimized] = useState<boolean>(false);
  const [isDrawerOpen, setIsDrawerOpen] = useState<boolean>(false);
  const [logs, setLogs] = useState<Record<string, string[]>>({});

  const eventSourcesRef = useRef<Record<string, EventSource>>({});

  const cleanupStream = useCallback((jobId: string) => {
    if (eventSourcesRef.current[jobId]) {
      eventSourcesRef.current[jobId].close();
      delete eventSourcesRef.current[jobId];
    }
  }, []);

  const cleanupAllStreams = useCallback(() => {
    Object.keys(eventSourcesRef.current).forEach((jobId) => {
      eventSourcesRef.current[jobId].close();
    });
    eventSourcesRef.current = {};
  }, []);

  const startJob = useCallback((jobId: string, initialTitle?: string) => {
    // If a stream already exists for this job, cleanup first
    cleanupStream(jobId);

    // Initial placeholder for this job
    const newJob: IngestJobInfo = {
      id: jobId,
      title: initialTitle || 'Ingesting to Lakehouse...',
      job_type: 'single',
      connection_id: '',
      source_name: '',
      source_type: '',
      status: 'running',
      mode: 'snapshot',
      started_at: new Date().toISOString(),
      total_tables: 1,
      completed_tables: 0,
      failed_tables: 0,
      percent: 0,
      total_rows: 0,
      logs: [],
      results: [],
      errors: [],
      is_cancelled: false,
    };

    setJobs((prev) => ({ ...prev, [jobId]: newJob }));
    setLogs((prev) => ({ ...prev, [jobId]: [] }));
    setActiveJobId(jobId);
    setIsDrawerOpen(true);
    setIsMinimized(false);

    const es = new EventSource(`/api/ingest/jobs/${jobId}/stream`);
    eventSourcesRef.current[jobId] = es;

    es.addEventListener('log', (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.text) {
          setLogs((prev) => ({
            ...prev,
            [jobId]: [...(prev[jobId] || []), payload.text],
          }));
        }
      } catch (err) {
        console.error(`Failed to parse SSE log event for job ${jobId}`, err);
      }
    });

    es.addEventListener('progress', (event) => {
      try {
        const payload = JSON.parse(event.data);
        setJobs((prev) => {
          const current = prev[jobId];
          if (!current) return prev;
          return {
            ...prev,
            [jobId]: {
              ...current,
              ...payload,
              percent: payload.percent ?? current.percent,
              status: payload.status ?? current.status,
              current_table: payload.current_table ?? current.current_table,
              current_phase: payload.current_phase ?? current.current_phase,
              completed_tables: payload.completed_tables ?? current.completed_tables,
              total_tables: payload.total_tables ?? current.total_tables,
              total_rows: payload.total_rows ?? current.total_rows,
            },
          };
        });
      } catch (err) {
        console.error(`Failed to parse SSE progress event for job ${jobId}`, err);
      }
    });

    es.addEventListener('complete', (event) => {
      try {
        const payload = JSON.parse(event.data);
        const jobData: IngestJobInfo = payload.job || payload;
        setJobs((prev) => ({
          ...prev,
          [jobId]: jobData,
        }));
        if (jobData.logs && jobData.logs.length > 0) {
          setLogs((prev) => ({
            ...prev,
            [jobId]: jobData.logs,
          }));
        }
      } catch (err) {
        console.error(`Failed to parse SSE complete event for job ${jobId}`, err);
      } finally {
        cleanupStream(jobId);
      }
    });

    es.addEventListener('cancelled', (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.job) {
          setJobs((prev) => ({
            ...prev,
            [jobId]: payload.job,
          }));
        }
      } catch (err) {
        console.error(`Failed to parse SSE cancelled event for job ${jobId}`, err);
      } finally {
        cleanupStream(jobId);
      }
    });

    es.onerror = () => {
      api.getIngestJob(jobId)
        .then((latest) => {
          setJobs((prev) => ({ ...prev, [jobId]: latest }));
          if (latest.status !== 'running') {
            cleanupStream(jobId);
          }
        })
        .catch(() => {});
    };
  }, [cleanupStream]);

  // On initial mount, probe for running background jobs to restore dock if user reloaded
  useEffect(() => {
    let isMounted = true;
    api.getActiveIngestJobs()
      .then((active) => {
        if (!isMounted || active.length === 0) return;
        active.forEach((job) => {
          startJob(job.id, job.title);
        });
        setIsMinimized(true); // Docked by default on reload
      })
      .catch(() => {});

    return () => {
      isMounted = false;
      cleanupAllStreams();
    };
  }, [startJob, cleanupAllStreams]);

  const selectJob = useCallback((jobId: string) => {
    setActiveJobId(jobId);
    setIsMinimized(false);
  }, []);

  const minimizeDrawer = useCallback(() => setIsMinimized(true), []);
  const restoreDrawer = useCallback(() => setIsMinimized(false), []);

  const dismissJob = useCallback((jobId: string) => {
    cleanupStream(jobId);
    setJobs((prev) => {
      const next = { ...prev };
      delete next[jobId];
      return next;
    });
    setLogs((prev) => {
      const next = { ...prev };
      delete next[jobId];
      return next;
    });

    setActiveJobId((prevActiveId) => {
      if (prevActiveId === jobId) {
        // Pick another job if available
        const remainingKeys = Object.keys(jobs).filter((id) => id !== jobId);
        if (remainingKeys.length === 0) {
          setIsDrawerOpen(false);
          return null;
        }
        return remainingKeys[0];
      }
      return prevActiveId;
    });
  }, [cleanupStream, jobs]);

  const clearCompleted = useCallback(() => {
    const activeKeys = Object.keys(jobs).filter(
      (id) => jobs[id].status === 'running'
    );
    const completedKeys = Object.keys(jobs).filter(
      (id) => jobs[id].status !== 'running'
    );

    completedKeys.forEach((id) => cleanupStream(id));

    setJobs((prev) => {
      const next: Record<string, IngestJobInfo> = {};
      activeKeys.forEach((id) => {
        next[id] = prev[id];
      });
      return next;
    });

    setLogs((prev) => {
      const next: Record<string, string[]> = {};
      activeKeys.forEach((id) => {
        next[id] = prev[id];
      });
      return next;
    });

    if (activeKeys.length === 0) {
      setIsDrawerOpen(false);
      setActiveJobId(null);
    } else if (!activeKeys.includes(activeJobId || '')) {
      setActiveJobId(activeKeys[0]);
    }
  }, [jobs, activeJobId, cleanupStream]);

  const cancelJob = useCallback(async (jobId: string) => {
    try {
      await api.cancelIngestJob(jobId);
    } catch (e) {
      console.error(`Failed to cancel job ${jobId}`, e);
    }
  }, []);

  const cancelAll = useCallback(async () => {
    const runningJobs = Object.values(jobs).filter((j) => j.status === 'running');
    await Promise.all(runningJobs.map((j) => api.cancelIngestJob(j.id)));
  }, [jobs]);

  const activeJob = activeJobId ? jobs[activeJobId] || null : null;
  const activeLogs = activeJobId ? logs[activeJobId] || [] : [];
  const runningCount = Object.values(jobs).filter((j) => j.status === 'running').length;

  return (
    <IngestContext.Provider
      value={{
        jobs,
        activeJobId,
        activeJob,
        isMinimized,
        isDrawerOpen,
        logs,
        activeLogs,
        runningCount,
        startJob,
        selectJob,
        minimizeDrawer,
        restoreDrawer,
        dismissJob,
        clearCompleted,
        cancelJob,
        cancelAll,
      }}
    >
      {children}
    </IngestContext.Provider>
  );
};

export const useIngestTracker = () => {
  const context = useContext(IngestContext);
  if (!context) {
    throw new Error('useIngestTracker must be used within an IngestProvider');
  }
  return context;
};
