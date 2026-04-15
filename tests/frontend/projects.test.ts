/**
 * Unit tests for static/js/projects.js archived-run search.
 *
 * Run with: npx vitest run tests/frontend/projects.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"},"h100":{"host":null,"gpu_type":"H100"}}</script>
    <div id="project-detail-title"></div>
    <input id="proj-search" value="" />
    <div id="proj-state-filters">
      <button class="hist-state-btn active" data-state="COMPLETED"></button>
      <button class="hist-state-btn active" data-state="FAILED"></button>
      <button class="hist-state-btn active" data-state="CANCELLED"></button>
      <button class="hist-state-btn active" data-state="TIMEOUT"></button>
      <button class="hist-state-btn active" data-state="RUNNING"></button>
      <button class="hist-state-btn active" data-state="PENDING"></button>
    </div>
    <div id="proj-stats-bar"></div>
    <div id="proj-live-section"></div>
    <div id="proj-hist-cards"></div>
    <div id="proj-pagination"></div>
    <div id="proj-archive-wrap" style="display:none"></div>
    <div id="proj-archive-sep"></div>
    <div id="settings-overlay"></div>
  `;
}

beforeAll(() => {
  renderDom();
  loadBrowserScripts(['utils.js', 'projects.js']);
});

beforeEach(() => {
  renderDom();
  vi.restoreAllMocks();
  sessionStorage.clear();
  (globalThis as any)._appTabs = [];
  (globalThis as any)._activeTabId = '';
  (globalThis as any).currentTab = 'project';
  (globalThis as any).allData = {};
  (globalThis as any)._activateView = vi.fn();
  (globalThis as any)._renderAppTabs = vi.fn();
  (globalThis as any)._persistTabs = vi.fn();
  (globalThis as any).renderCard = vi.fn(() => '<div class="card"></div>');
  (globalThis as any)._saveProgressCache = vi.fn();
  (globalThis as any).toastLoading = vi.fn(() => ({ done: vi.fn() }));
});

afterEach(() => {
  vi.useRealTimers();
});

declare const openProject: (projectName: string, fromTab?: boolean) => Promise<void>;
declare const projectSearchChanged: () => void;
declare const _fetchProjectHistory: (showToast?: boolean) => Promise<void>;

describe('project archived run search', () => {
  it('queries archived runs through the same history API search path', async () => {
    const fetchMock = vi.fn((url: string) => {
      const target = String(url);
      if (target === '/api/settings') {
        return Promise.resolve({ json: async () => ({ projects: {} }) });
      }
      if (target === '/api/jobs') {
        return Promise.resolve({ json: async () => ({}) });
      }
      if (target.startsWith('/api/history?')) {
        return Promise.resolve({
          json: async () => ([
            {
              cluster: 'h100',
              job_id: '101',
              job_name: 'hle_text-qwen35-no-tool-r7',
              run_name: 'text-qwen35-no-tool-r7',
              state: 'COMPLETED',
              depends_on: [],
              dep_details: [],
            },
          ]),
        });
      }
      return Promise.reject(new Error(`Unexpected fetch: ${target}`));
    });
    (globalThis as any).fetch = fetchMock;

    await openProject('hle', true);
    await _fetchProjectHistory(false);

    fetchMock.mockClear();
    vi.useFakeTimers();
    (document.getElementById('proj-search') as HTMLInputElement).value = 'qwen35';
    projectSearchChanged();
    await vi.advanceTimersByTimeAsync(200);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0][0])).toContain('/api/history?');
    expect(String(fetchMock.mock.calls[0][0])).toContain('project=hle');
    expect(String(fetchMock.mock.calls[0][0])).toContain('q=qwen35');
  });

  it('keeps same-run jobs grouped and collapsible in archived search', async () => {
    const fetchMock = vi.fn((url: string) => {
      const target = String(url);
      if (target === '/api/settings') {
        return Promise.resolve({ json: async () => ({ projects: {} }) });
      }
      if (target === '/api/jobs') {
        return Promise.resolve({ json: async () => ({}) });
      }
      if (target.startsWith('/api/history?')) {
        return Promise.resolve({
          json: async () => ([
            {
              cluster: 'h100',
              job_id: '101',
              job_name: 'hle_text-qwen35-no-tool-r7',
              run_name: 'text-qwen35-no-tool-r7',
              run_id: 77,
              state: 'COMPLETED',
              depends_on: [],
              dep_details: [],
            },
            {
              cluster: 'h100',
              job_id: '102',
              job_name: 'hle_text-qwen35-no-tool-r7-judge-rs0',
              run_name: 'text-qwen35-no-tool-r7',
              run_id: 77,
              state: 'COMPLETED',
              depends_on: [],
              dep_details: [],
            },
          ]),
        });
      }
      return Promise.reject(new Error(`Unexpected fetch: ${target}`));
    });
    (globalThis as any).fetch = fetchMock;

    await openProject('hle', true);
    (document.getElementById('proj-search') as HTMLInputElement).value = 'qwen35';
    await _fetchProjectHistory(false);

    expect(document.querySelectorAll('#proj-hist-cards tr.group-head-row').length).toBe(1);
    expect(document.querySelectorAll('#proj-hist-cards tr.hist-compact').length).toBe(2);
    expect(document.querySelector('#proj-hist-cards [data-group-chevron="h100:101"]')).not.toBeNull();
    expect(Array.from(document.querySelectorAll('#proj-hist-cards tr.hist-compact')).every(
      row => (row as HTMLElement).style.display === 'none'
    )).toBe(true);
  });
});
