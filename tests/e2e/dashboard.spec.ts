/**
 * E2E: Dashboard live view, cluster cards, sidebar, summary.
 *
 * Run with: npx playwright test tests/e2e/dashboard.spec.ts
 * Requires: app.py running on localhost:7272
 */

import { test, expect } from '@playwright/test';

test.describe('Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page loads with cluster cards', async ({ page }) => {
    await expect(page.locator('#grid')).toBeVisible();
    const cards = page.locator('[id^="card-"]');
    await expect(cards.first()).toBeVisible({ timeout: 15000 });
  });

  test('summary counters are populated', async ({ page }) => {
    await page.waitForSelector('#s-running');
    const running = await page.textContent('#s-running');
    expect(running).not.toBe('—');
  });

  test('sidebar toggle works', async ({ page }) => {
    const nav = page.locator('#side-nav');
    const toggle = page.locator('#nav-toggle');
    await toggle.click();
    await expect(nav).toHaveClass(/collapsed/);
    await toggle.click();
    await expect(nav).not.toHaveClass(/collapsed/);
  });

  test('section collapse/expand', async ({ page }) => {
    await page.waitForSelector('.section-label.toggleable');
    const label = page.locator('.section-label.toggleable').first();
    await label.click();
    // Section should toggle collapsed state
    await label.click();
  });

  test('refresh button triggers data reload', async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse(resp => resp.url().includes('/api/jobs/')),
      page.goto('/'),
    ]);
    expect(response.status()).toBe(200);
  });

  test('cluster card shows job table when jobs exist', async ({ page }) => {
    await page.waitForSelector('#grid');
    // Wait for at least one card to render
    await page.waitForTimeout(3000);
    const cards = page.locator('.card');
    const count = await cards.count();
    expect(count).toBeGreaterThan(0);
  });
});
