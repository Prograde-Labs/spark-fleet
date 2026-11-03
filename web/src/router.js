import { createRouter, createWebHashHistory } from 'vue-router'
import { api } from './api'

const routes = [
  {
    path: '/',
    name: 'dashboard',
    component: () => import('./views/Dashboard.vue'),
    meta: { title: 'Fleet Overview', code: 'OPS-01' },
  },
  {
    path: '/deployments',
    name: 'deployments',
    component: () => import('./views/Deployments.vue'),
    meta: { title: 'Deployments', code: 'OPS-02' },
  },
  {
    path: '/recipes',
    name: 'recipes',
    component: () => import('./views/RecipeEditor.vue'),
    meta: { title: 'Recipes', code: 'OPS-03' },
  },
  {
    path: '/models',
    name: 'models',
    component: () => import('./views/Models.vue'),
    meta: { title: 'Model Vault', code: 'OPS-04' },
  },
  {
    path: '/nodes',
    name: 'nodes',
    component: () => import('./views/Nodes.vue'),
    meta: { title: 'Node Registry', code: 'OPS-05' },
  },
  {
    path: '/activity',
    name: 'activity',
    component: () => import('./views/Activity.vue'),
    meta: { title: 'Activity Log', code: 'OPS-06' },
  },
  {
    path: '/import',
    name: 'import',
    component: () => import('./views/Import.vue'),
    meta: { title: 'Import', code: 'OPS-07' },
  },
  {
    path: '/settings',
    name: 'settings',
    component: () => import('./views/Settings.vue'),
    meta: { title: 'Settings', code: 'OPS-08' },
  },
  {
    path: '/login',
    name: 'login',
    component: () => import('./views/Login.vue'),
    meta: { title: 'Operator Login', code: 'AUTH-00', layout: 'bare' },
  },
  {
    path: '/:pathMatch(.*)*',
    redirect: '/',
  },
]

const router = createRouter({
  history: createWebHashHistory(),
  routes,
  scrollBehavior: () => ({ top: 0 }),
})

router.beforeEach(async (to) => {
  if (to.meta?.layout === 'bare') return true
  try {
    const session = await api.session()
    if (session.enabled && !session.authenticated) return { name: 'login' }
  } catch {
    // Controller unreachable: let the view show its own error state.
  }
  return true
})

router.afterEach((to) => {
  const title = to.meta?.title || 'Fleet'
  document.title = `Spark Fleet // ${title}`
})

export default router
