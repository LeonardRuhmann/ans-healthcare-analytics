import { ref, watch } from 'vue'
import api from '../services/api'

export function useOperadoras() {
    const operadoras = ref([])
    const pagination = ref({
        page: 1,
        limit: 10,
        total: 0,
        total_pages: 0
    })
    const filters = ref({
        search: ''
    })
    const loading = ref(false)
    const error = ref(null)

    const fetchOperadoras = async () => {
        loading.value = true
        error.value = null
        try {
            const params = {
                page: pagination.value.page,
                limit: pagination.value.limit,
                search: filters.value.search
            }
            const response = await api.get('/operadoras', { params })
            operadoras.value = response.data.data
            pagination.value = response.data.pagination
        } catch (err) {
            console.error(err)
            if (err.response?.data?.detail) {
                error.value = err.response.data.detail
            } else {
                error.value = 'Failed to load operators.'
            }
        } finally {
            loading.value = false
        }
    }

    // Watch for page changes
    watch(() => pagination.value.page, () => {
        fetchOperadoras()
    })

    // Watch for search changes with debounce
    let timeout
    watch(() => filters.value.search, () => {
        clearTimeout(timeout)
        timeout = setTimeout(() => {
            pagination.value.page = 1 // Reset to page 1
            fetchOperadoras()
        }, 500)
    })

    return {
        operadoras,
        pagination,
        filters,
        loading,
        error,
        fetchOperadoras
    }
}
