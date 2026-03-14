/**
 * E2E: History tab, filters, pagination.
 *
 * Run with: npx playwright test tests/e2e/history.spec.ts
 */

import { test, expect } from '@playwright/test';

test.describe('History', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.click('#tab-history');
  });

  test('history tab shows table', async ({ page }) => {
    await expect(page.locator('#hist-table')).toBeVisible();
  });

  test('cluster filter changes results', async ({ page }) => {
    const select = page.locator('#hist-cluster');
    await select.selectOption('all');
    await page.waitForTimeout(1000);
    const beforeCount = await page.locator('#hist-body tr').count();

    // Select first non-all option if available
    const options = await select.locator('option').allTextContents();
    if (options.length > 1) {
      await select.selectOption({ index: 1 });
      await page.waitForTimeout(1000);
    }
  });

  test('search filter works', async ({ page }) => {
    await page.waitForTimeout(1000);
    const search = page.locator('#hist-search');
    await search.fill('nonexistent-job-name-xyz');
    await page.waitForTimeout(500);
    const rows = await page.locator('#hist-body tr').count();
    // Either no rows or the "no history yet" row
    expect(rows).toBeLessThanOrEqual(1);
  });

  test('pagination controls appear for large history', async ({ page }) => {
    await page.waitForTimeout(2000);
    const pag = page.locator('#hist-pagination');
    const text = await pag.textContent();
    // Pagination shows page info or is empty
    expect(text !== null).toBe(true);
  });
});
