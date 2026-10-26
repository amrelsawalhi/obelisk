import React, { useState, useEffect, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import {
  Save,
  X,
  AlertCircle,
  Check,
  FolderOpen,
  Code2,
} from 'lucide-react';


import { api } from '../../lib/api';
import type { FileNode, FileTreeResponse } from '../../lib/types';
import { FileTree } from './FileTree';
import { ConfirmDialog } from '../common/ConfirmDialog';
import type { ConfirmRequest } from '../common/ConfirmDialog';

interface OpenFile {
  path: string;
  name: string;
  content: string;
  originalContent: string;
  isDirty: boolean;
  extension: string;
  size: number;
  modifiedAt?: string;
}

const getLanguageFromExtension = (ext: string): string => {
  switch (ext.toLowerCase()) {
    case '.py':
      return 'python';
    case '.sql':
      return 'sql';
    case '.yml':
    case '.yaml':
      return 'yaml';
    case '.json':
      return 'json';
    case '.md':
      return 'markdown';
    case '.sh':
    case '.bash':
      return 'shell';
    case '.html':
      return 'html';
    case '.css':
      return 'css';
    case '.js':
    case '.jsx':
      return 'javascript';
    case '.ts':
    case '.tsx':
      return 'typescript';
    default:
      return 'plaintext';
  }
};

export const FileManagerView: React.FC = () => {
  const [treeData, setTreeData] = useState<FileNode[]>([]);
  const [loadingTree, setLoadingTree] = useState(true);
  const [openFiles, setOpenFiles] = useState<OpenFile[]>([]);
  const [activePath, setActivePath] = useState<string | null>(null);

  // Status & feedback
  const [saving, setSaving] = useState(false);
  const [statusMessage, setStatusMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [confirmRequest, setConfirmRequest] = useState<ConfirmRequest | null>(null);

  const showStatus = useCallback((type: 'success' | 'error', text: string, duration = 4000) => {
    setStatusMessage({ type, text });
    setTimeout(() => {
      setStatusMessage((current) => (current?.text === text ? null : current));
    }, duration);
  }, []);

  // Clear any status message when active tab/file changes
  useEffect(() => {
    setStatusMessage(null);
  }, [activePath]);

  // Modals state
  const [modalState, setModalState] = useState<{
    type: 'create' | 'rename' | 'delete' | null;
    parentPath?: string;
    itemType?: 'file' | 'directory';
    targetNode?: FileNode;
    inputValue?: string;
  }>({ type: null });

  const activeFile = openFiles.find((f) => f.path === activePath) || null;

  // Load tree
  const fetchTree = useCallback(async () => {
    try {
      setLoadingTree(true);
      const res: FileTreeResponse = await api.getFileTree();
      setTreeData(res.children || []);
    } catch (err: any) {
      showStatus('error', err?.response?.data?.detail || 'Failed to load file tree');
    } finally {
      setLoadingTree(false);
    }
  }, [showStatus]);

  useEffect(() => {
    fetchTree();
  }, [fetchTree]);

  // Open a file
  const handleSelectFile = async (node: FileNode) => {
    if (node.type === 'directory') return;

    // Check if already open
    const existing = openFiles.find((f) => f.path === node.path);
    if (existing) {
      setActivePath(node.path);
      return;
    }

    try {
      const res = await api.getFileContent(node.path);
      const newFile: OpenFile = {
        path: res.path,
        name: res.name,
        content: res.content,
        originalContent: res.content,
        isDirty: false,
        extension: res.extension,
        size: res.size,
        modifiedAt: res.modified_at,
      };
      setOpenFiles((prev) => [...prev, newFile]);
      setActivePath(res.path);
    } catch (err: any) {
      showStatus('error', err?.response?.data?.detail || `Failed to open ${node.name}`);
    }
  };


  // Close tab
  const closeTabNow = (path: string) => {
    const nextFiles = openFiles.filter((f) => f.path !== path);
    setOpenFiles(nextFiles);

    if (activePath === path) {
      if (nextFiles.length > 0) {
        setActivePath(nextFiles[nextFiles.length - 1].path);
      } else {
        setActivePath(null);
      }
    }
  };

  const handleCloseTab = (path: string, e?: React.MouseEvent) => {
    if (e) e.stopPropagation();
    const file = openFiles.find((f) => f.path === path);
    if (file?.isDirty) {
      setConfirmRequest({
        title: `Discard changes to "${file.name}"?`,
        body: 'The buffer has unsaved edits. Closing the tab throws them away.',
        confirmLabel: 'Discard and close',
        destructive: true,
        onConfirm: () => closeTabNow(path),
      });
      return;
    }
    closeTabNow(path);
  };

  // Update content of active file
  const handleContentChange = (value: string | undefined) => {
    const val = value ?? '';
    setOpenFiles((prev) =>
      prev.map((f) => {
        if (f.path === activePath) {
          return {
            ...f,
            content: val,
            isDirty: val !== f.originalContent,
          };
        }
        return f;
      })
    );
  };

  // Save current file
  const handleSave = async () => {
    if (!activeFile || saving) return;
    try {
      setSaving(true);
      const res = await api.saveFile(activeFile.path, activeFile.content);
      setOpenFiles((prev) =>
        prev.map((f) => {
          if (f.path === activeFile.path) {
            return {
              ...f,
              originalContent: f.content,
              isDirty: false,
              size: res.size,
              modifiedAt: res.modified_at,
            };
          }
          return f;
        })
      );
      showStatus('success', `Saved ${activeFile.name}`);
    } catch (err: any) {
      showStatus('error', err?.response?.data?.detail || 'Failed to save file');
    } finally {
      setSaving(false);
    }
  };


  // Keyboard shortcut listener for Ctrl+S / Cmd+S
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        handleSave();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [activeFile, saving]);

  // Open create modal
  const handleOpenCreate = (parentPath: string, type: 'file' | 'directory') => {
    setModalState({
      type: 'create',
      parentPath,
      itemType: type,
      inputValue: '',
    });
  };

  // Open rename modal
  const handleOpenRename = (node: FileNode) => {
    setModalState({
      type: 'rename',
      targetNode: node,
      inputValue: node.name,
    });
  };

  // Open delete modal
  const handleOpenDelete = (node: FileNode) => {
    setModalState({
      type: 'delete',
      targetNode: node,
    });
  };

  // Submit modal action
  const handleModalSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const { type, parentPath, itemType, targetNode, inputValue } = modalState;

    if (type === 'create' && inputValue && itemType) {
      const fullPath = parentPath ? `${parentPath}/${inputValue.trim()}` : inputValue.trim();
      try {
        await api.createFile(fullPath, itemType);
        await fetchTree();
        setModalState({ type: null });
        if (itemType === 'file') {
          handleSelectFile({
            name: inputValue.trim(),
            path: fullPath,
            type: 'file',
            extension: '.' + inputValue.split('.').pop(),
          });
        }
      } catch (err: any) {
        showStatus('error', err?.response?.data?.detail || 'Error creating item');
      }
    } else if (type === 'rename' && targetNode && inputValue) {
      const parentDir = targetNode.path.includes('/')
        ? targetNode.path.substring(0, targetNode.path.lastIndexOf('/'))
        : '';
      const newPath = parentDir ? `${parentDir}/${inputValue.trim()}` : inputValue.trim();
      try {
        await api.renameFile(targetNode.path, newPath);
        // Update any open file tabs
        setOpenFiles((prev) =>
          prev.map((f) =>
            f.path === targetNode.path
              ? { ...f, path: newPath, name: inputValue.trim() }
              : f
          )
        );
        if (activePath === targetNode.path) {
          setActivePath(newPath);
        }
        await fetchTree();
        setModalState({ type: null });
      } catch (err: any) {
        showStatus('error', err?.response?.data?.detail || 'Error renaming item');
      }
    } else if (type === 'delete' && targetNode) {
      try {
        await api.deleteFile(targetNode.path);
        // Clear status and close if open
        setStatusMessage(null);
        // The file no longer exists, so close its tab without asking about unsaved edits.
        closeTabNow(targetNode.path);
        await fetchTree();
        setModalState({ type: null });
      } catch (err: any) {
        showStatus('error', err?.response?.data?.detail || 'Error deleting item');
      }
    }
  };

  return (
    <div className="flex-1 flex h-full min-h-0 bg-white overflow-hidden select-none">
      {/* Left Pane: File Tree Explorer */}
      <div className="w-72 border-r border-slate-200 flex flex-col h-full bg-slate-50/40 shrink-0">
        <FileTree
          nodes={treeData}
          loading={loadingTree}
          selectedPath={activePath}
          onSelectFile={handleSelectFile}
          onRefresh={fetchTree}
          onCreate={handleOpenCreate}
          onRename={handleOpenRename}
          onDelete={handleOpenDelete}
        />

      </div>

      {/* Right Pane: Code Editor Studio */}
      <div className="flex-1 flex flex-col h-full min-w-0 bg-white">
        {/* Tab Bar */}
        <div className="h-10 border-b border-slate-200 bg-slate-100/70 flex items-center px-1 overflow-x-auto shrink-0 select-none">
          {openFiles.length === 0 ? (
            <div className="text-xs text-slate-400 px-3 py-1 flex items-center gap-1.5 italic">
              <FolderOpen className="w-3.5 h-3.5" /> No file open
            </div>
          ) : (
            openFiles.map((file) => {
              const isActive = file.path === activePath;
              return (
                <div
                  key={file.path}
                  onClick={() => setActivePath(file.path)}
                  className={`group flex items-center gap-2 px-3 py-1.5 text-xs font-medium cursor-pointer border-r border-slate-200 transition-colors ${
                    isActive
                      ? 'bg-white text-slate-900 border-t-2 border-t-sky-600 shadow-xs'
                      : 'text-slate-600 hover:bg-slate-200/60 hover:text-slate-900'
                  }`}
                >
                  <span className="truncate max-w-[140px]">{file.name}</span>
                  {file.isDirty ? (
                    <span className="w-2 h-2 rounded-full bg-amber-500 shrink-0" title="Unsaved changes" />
                  ) : null}
                  <button
                    onClick={(e) => handleCloseTab(file.path, e)}
                    className="p-0.5 rounded-full hover:bg-slate-200 text-slate-400 hover:text-slate-700"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              );
            })
          )}
        </div>

        {/* Toolbar & Breadcrumbs */}
        {activeFile && (
          <div className="h-10 border-b border-slate-200 px-4 flex items-center justify-between bg-white text-xs text-slate-600 shrink-0">
            <div className="flex items-center gap-2 truncate">
              <span className="font-mono text-slate-400">git_repo /</span>
              <span className="font-mono font-medium text-slate-800 truncate">
                {activeFile.path}
              </span>
              <span className="px-1.5 py-0.5 rounded bg-slate-100 text-slate-500 font-mono text-[10px] uppercase">
                {getLanguageFromExtension(activeFile.extension)}
              </span>
            </div>

            <div className="flex items-center gap-3">
              {statusMessage && (
                <div
                  className={`flex items-center gap-1.5 px-2.5 py-0.5 rounded text-xs font-medium transition-all ${
                    statusMessage.type === 'success'
                      ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                      : 'bg-rose-50 text-rose-700 border border-rose-200'
                  }`}
                >
                  {statusMessage.type === 'success' ? (
                    <Check className="w-3.5 h-3.5 shrink-0 text-emerald-600" />
                  ) : (
                    <AlertCircle className="w-3.5 h-3.5 shrink-0 text-rose-600" />
                  )}
                  <span>{statusMessage.text}</span>
                  <button
                    onClick={() => setStatusMessage(null)}
                    title="Dismiss"
                    className="ml-1 p-0.5 rounded hover:bg-black/5 opacity-70 hover:opacity-100"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              )}


              <button
                onClick={handleSave}
                disabled={saving || !activeFile.isDirty}
                className={`flex items-center gap-1.5 px-3 py-1 rounded text-xs font-medium transition-colors ${
                  activeFile.isDirty
                    ? 'bg-sky-600 text-white hover:bg-sky-700 shadow-xs'
                    : 'bg-slate-100 text-slate-400 cursor-not-allowed'
                }`}
              >
                <Save className="w-3.5 h-3.5" />
                <span>{saving ? 'Saving...' : 'Save (Ctrl+S)'}</span>
              </button>
            </div>
          </div>
        )}

        {/* Editor Body or Welcome State */}
        <div className="flex-1 min-h-0 relative">
          {activeFile ? (
            <Editor
              height="100%"
              path={activeFile.path}
              language={getLanguageFromExtension(activeFile.extension)}
              value={activeFile.content}
              onChange={handleContentChange}
              theme="vs"
              options={{
                fontSize: 13,
                fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, monospace",
                lineNumbers: 'on',
                minimap: { enabled: true },
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                automaticLayout: true,
                tabSize: 2,
                insertSpaces: true,
              }}
            />
          ) : (
            <div className="h-full flex flex-col items-center justify-center p-8 text-center bg-slate-50/30">
              <div className="w-16 h-16 rounded-2xl bg-sky-50 text-sky-600 flex items-center justify-center mb-4 shadow-sm border border-sky-100">
                <Code2 className="w-8 h-8" />
              </div>
              <h3 className="text-base font-semibold text-slate-800 mb-1">
                git_repo Code Studio
              </h3>
              <p className="text-xs text-slate-500 max-w-md leading-relaxed">
                Select any dbt model, Python script, or Marimo notebook from the explorer on the left to edit and version control code in real time.
              </p>
            </div>

          )}
        </div>
      </div>

      {/* Action Dialog / Modal */}
      {modalState.type && (
        <div className="fixed inset-0 z-50 bg-slate-900/40 backdrop-blur-xs flex items-center justify-center p-4">
          <div className="bg-white rounded-xl shadow-xl border border-slate-200 max-w-sm w-full p-5 overflow-hidden animate-in fade-in zoom-in-95 duration-100">
            <h3 className="font-semibold text-slate-800 text-sm mb-1">
              {modalState.type === 'create' &&
                `New ${modalState.itemType === 'directory' ? 'Folder' : 'File'}`}
              {modalState.type === 'rename' && 'Rename Item'}
              {modalState.type === 'delete' && 'Delete Item'}
            </h3>
            <p className="text-xs text-slate-500 mb-4">
              {modalState.type === 'create' &&
                `Create a new ${modalState.itemType} inside ${
                  modalState.parentPath ? `git_repo/${modalState.parentPath}` : 'git_repo root'
                }`}
              {modalState.type === 'rename' &&
                `Enter a new name for ${modalState.targetNode?.name}`}
              {modalState.type === 'delete' &&
                `Are you sure you want to delete "${modalState.targetNode?.name}"? This action cannot be undone.`}
            </p>

            {modalState.type !== 'delete' ? (
              <form onSubmit={handleModalSubmit}>
                <input
                  type="text"
                  autoFocus
                  required
                  placeholder={
                    modalState.itemType === 'file' ? 'e.g. transform.py, model.sql' : 'e.g. analytics'
                  }
                  value={modalState.inputValue || ''}
                  onChange={(e) =>
                    setModalState((prev) => ({ ...prev, inputValue: e.target.value }))
                  }
                  className="w-full px-3 py-2 border border-slate-300 rounded-lg text-xs focus:outline-none focus:ring-2 focus:ring-sky-500 mb-4"
                />
                <div className="flex justify-end gap-2">
                  <button
                    type="button"
                    onClick={() => setModalState({ type: null })}
                    className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="px-3 py-1.5 text-xs font-medium bg-sky-600 text-white rounded-lg hover:bg-sky-700"
                  >
                    {modalState.type === 'create' ? 'Create' : 'Rename'}
                  </button>
                </div>
              </form>
            ) : (
              <div className="flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setModalState({ type: null })}
                  className="px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 rounded-lg"
                >
                  Cancel
                </button>
                <button
                  onClick={handleModalSubmit}
                  className="px-3 py-1.5 text-xs font-medium bg-rose-600 text-white rounded-lg hover:bg-rose-700"
                >
                  Delete
                </button>
              </div>
            )}
          </div>
        </div>
      )}

      <ConfirmDialog request={confirmRequest} onClose={() => setConfirmRequest(null)} />
    </div>
  );
};
